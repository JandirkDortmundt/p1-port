package main

import (
	"context"       // For context management and cancellation
	"database/sql"  // Standard Go SQL package
	"encoding/json" // For JSON deserialization
	"fmt"           // For printing output
	"log"           // For logging messages
	"os"            // For signal handling and environment variables
	"os/signal"     // For signal handling
	//"strconv"      // For converting string to int
	"syscall" // For signal handling (SIGTERM)
	"time"    // For time durations

	// Third-party libraries
	"github.com/joho/godotenv"          // For loading environment variables from .env
	_ "github.com/marcboeker/go-duckdb" // Correct DuckDB driver import for database/sql
	"github.com/segmentio/kafka-go"     // For Kafka consumer
)

// --- Configuration Variables ---
var (
	kafkaBroker      string // Kafka broker address (e.g., "localhost:9092")
	electricityTopic string // Kafka topic for electricity data (e.g., "p1-electricity")
	gasTopic         string // Kafka topic for gas data (e.g., "p1-gas")
	duckdbPath       string // Path to the DuckDB database file
)

// --- Data Structures ---

// Represents a message received from Kafka, including its offset and topic/partition
type KafkaMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Value     []byte    // The raw JSON message value
	Timestamp time.Time // The timestamp associated with the message in Kafka
}

// --- Main Function ---

func main() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: Could not load .env file. Ensure environment variables are set.")
	}

	// Read configuration from environment variables
	kafkaBroker = os.Getenv("KAFKA_BROKER_ADDRESS")
	electricityTopic = os.Getenv("KAFKA_ELECTRICITY_TOPIC")
	gasTopic = os.Getenv("KAFKA_GAS_TOPIC")
	duckdbPath = os.Getenv("DUCKDB_PATH")

	if kafkaBroker == "" || electricityTopic == "" || gasTopic == "" {
		log.Fatal("Kafka environment variables (KAFKA_BROKER_ADDRESS, KAFKA_ELECTRICITY_TOPIC, KAFKA_GAS_TOPIC) not set!")
	}
	if duckdbPath == "" {
		log.Fatal("DUCKDB_PATH environment variable not set!")
	}

	log.Printf("Kafka Broker Address: %s", kafkaBroker)
	log.Printf("Electricity Topic: %s", electricityTopic)
	log.Printf("Gas Topic: %s", gasTopic)
	log.Printf("DuckDB Path: %s", duckdbPath)
	log.Println("Starting P1 Kafka consumer to DuckDB...")

	// --- DuckDB Setup ---
	// Open the DuckDB database connection
	// Use the correct driver name "duckdb"
	db, err := sql.Open("duckdb", duckdbPath)
	if err != nil {
		log.Fatalf("❌ Failed to open DuckDB database at %s: %v", duckdbPath, err)
	}
	defer func() {
		log.Println("Closing DuckDB connection...")
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("Error closing DuckDB connection: %v", closeErr)
		} else {
			log.Println("DuckDB connection closed successfully.")
		}
	}()

	// Initialize the necessary tables in DuckDB
	err = initializeDuckDB(db)
	if err != nil {
		log.Fatalf("❌ Failed to initialize DuckDB tables: %v", err)
	}
	log.Println("✅ DuckDB tables initialized.")
	// --- End DuckDB Setup ---

	// --- Signal Handling ---
	// Create a context that will be cancelled on shutdown signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancel is called when main exits

	// Create a channel to listen for OS signals
	sigChan := make(chan os.Signal, 1)
	// Notify the channel for Interrupt (Ctrl+C) and Terminate signals
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start a goroutine that waits for a signal and cancels the context
	go func() {
		<-sigChan // Block until a signal is received
		log.Println("Received interrupt signal. Initiating shutdown...")
		cancel() // Cancel the context
	}()
	// --- End Signal Handling ---

	// --- Kafka Consumer Setup ---
	// Read the last processed offsets from DuckDB
	startOffsets, err := getLastProcessedOffsets(db, []string{electricityTopic, gasTopic})
	if err != nil {
		log.Fatalf("❌ Failed to get last processed offsets from DuckDB: %v", err)
	}

	// Create a new Kafka reader for the specified topics and broker
	// GroupID is important for consumer groups. Use a unique ID for this application.
	readerConfig := kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},                // List of Kafka brokers
		GroupID:     "p1-data-consumer-group",             // Consumer group ID. MUST be unique for this consumer group.
		GroupTopics: []string{electricityTopic, gasTopic}, // Topics to subscribe to
		MinBytes:    10e3,                                 // 10KB
		MaxBytes:    10e6,                                 // 10MB
		// Optional: Set a CommitInterval to automatically commit offsets
		// CommitInterval: 1 * time.Second, // Using automatic commits
		Logger: log.New(os.Stdout, "KAFKA_CONSUMER ", log.LstdFlags), // Add Kafka consumer-specific logging
		// Note: We are NOT setting StartOffset here directly in ReaderConfig.
		// We will seek to the correct offset after the reader is created.
	}

	reader := kafka.NewReader(readerConfig)

	// Ensure the reader is closed when the main function exits
	defer func() {
		log.Println("Closing Kafka reader...")
		// Use the context with a timeout for closing the reader
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if closeErr := reader.Close(); closeErr != nil {
			log.Printf("❌ Failed to close Kafka reader: %v", closeErr)
		} else {
			log.Println("Kafka reader closed successfully.")
		}
	}()

	// Seek to the last processed offset for each topic/partition
	// This happens *after* the reader is created but before the consumption loop starts.
	for topic, partitions := range startOffsets {
		for partition, offset := range partitions {
			// Seek to the offset *after* the last processed one
			seekOffset := offset + 1
			// If the offset was -1 (kafka.FirstOffset), seek to the earliest available offset
			if offset == -1 {
				seekOffset = kafka.FirstOffset // Use kafka.FirstOffset to ensure starting from the beginning
				log.Printf("Seeking consumer group '%s' for topic '%s' partition %d to earliest available offset (%d)",
					readerConfig.GroupID, topic, partition, seekOffset)
			} else {
				log.Printf("Seeking consumer group '%s' for topic '%s' partition %d to offset %d",
					readerConfig.GroupID, topic, partition, seekOffset)
			}

			// Use a context for the seek operation
			seekCtx, seekCancel := context.WithTimeout(ctx, 5*time.Second)
			err := reader.Seek(seekCtx, kafka.TopicPartition{
				Topic:     topic,
				Partition: partition,
				Offset:    seekOffset,
			})
			seekCancel() // Cancel seek context

			if err != nil {
				// If seeking fails, it might mean the offset is too old and has been
				// removed by Kafka's retention policy. The consumer will start from the
				// earliest available offset in this case. Log a warning but continue.
				log.Printf("⚠️ Warning: Failed to seek consumer group '%s' for topic '%s' partition %d to offset %d: %v. Consumer might start from a different offset.",
					readerConfig.GroupID, topic, partition, seekOffset, err)
				// Note: If seeking to kafka.FirstOffset also fails, it indicates a more
				// fundamental issue with the reader or broker connection.
			}
		}
	}

	log.Println("Kafka consumer started. Waiting for messages...")

	// --- Message Consumption Loop ---
	// We will collect messages in a batch before writing to DuckDB
	messageBatch := []KafkaMessage{}
	batchSize := 10                           // Process messages in batches of 10 (adjust as needed)
	batchTimeout := 1 * time.Second           // Or process batch after 1 second (adjust as needed)
	batchTimer := time.NewTimer(batchTimeout) // Timer for batching

	for {
		select {
		case <-ctx.Done():
			// Context was cancelled (shutdown signal received)
			log.Println("Context cancelled, exiting message consumption loop.")
			// Process any remaining messages in the batch before exiting
			if len(messageBatch) > 0 {
				log.Printf("Processing remaining batch of %d messages before exiting...", len(messageBatch))
				if writeErr := writeBatchToDuckDB(db, messageBatch); writeErr != nil {
					log.Printf("❌ Error writing final batch to DuckDB: %v", writeErr)
				} else {
					log.Println("Successfully wrote final batch to DuckDB.")
				}
				messageBatch = nil // Clear the batch
			}
			return // Exit the main function

		case <-batchTimer.C:
			// Batch timeout occurred, process the current batch
			if len(messageBatch) > 0 {
				log.Printf("Batch timeout: Processing batch of %d messages...", len(messageBatch))
				// Stop the timer before processing the batch to prevent it from firing prematurely
				if !batchTimer.Stop() {
					select {
					case <-batchTimer.C:
						// Drain the channel if the timer had already fired
					default:
					}
				}
				if writeErr := writeBatchToDuckDB(db, messageBatch); writeErr != nil {
					log.Printf("❌ Error writing batch to DuckDB: %v", writeErr)
				} else {
					log.Println("Successfully wrote batch to DuckDB.")
				}
				messageBatch = nil // Clear the batch
			}
			// Reset the batch timer for the next batch
			batchTimer.Reset(batchTimeout)

		default:
			// No shutdown signal or batch timeout, attempt to read a message
			// Create a context with a short timeout for the ReadMessage call itself.
			// This allows the select statement to cycle and check the batch timer/ctx.Done().
			readMsgCtx, readMsgCancel := context.WithTimeout(ctx, 100*time.Millisecond) // Use a short timeout for polling
			msg, err := reader.ReadMessage(readMsgCtx)
			readMsgCancel() // Cancel the timeout context immediately after ReadMessage returns

			if err != nil {
				// Check if the error is specifically context cancellation or timeout
				if err == context.Canceled || err == context.DeadlineExceeded {
					// This is expected if the main context is cancelled or the read timeout occurs.
					// The select statement will handle the exit via <-ctx.Done() or the batch timer.
					continue // Continue to the next loop iteration
				}
				// Handle other unexpected errors during reading
				log.Printf("⚠️ Unexpected error reading message from Kafka: %v", err)
				// Depending on the error, you might want to sleep briefly or implement a retry strategy
				time.Sleep(1 * time.Second) // Simple sleep to avoid tight loop on persistent errors
				continue                    // Go to the next iteration of the loop
			}

			// --- Process Received Message ---
			// If we reached here, a message was successfully read.
			log.Printf("Received message from topic %s, partition %d, offset %d",
				msg.Topic, msg.Partition, msg.Offset)

			// Add the message to the batch
			messageBatch = append(messageBatch, KafkaMessage{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Value:     msg.Value,
				Timestamp: msg.Time, // Kafka message timestamp
			})

			// If the batch is full, process it immediately
			if len(messageBatch) >= batchSize {
				log.Printf("Batch size reached: Processing batch of %d messages...", len(messageBatch))
				// Stop the timer before processing the batch to prevent it from firing prematurely
				if !batchTimer.Stop() {
					select {
					case <-batchTimer.C:
						// Drain the channel if the timer had already fired
					default:
					}
				}
				if writeErr := writeBatchToDuckDB(db, messageBatch); writeErr != nil {
					log.Printf("❌ Error writing batch to DuckDB: %v", writeErr)
				} else {
					log.Println("Successfully wrote batch to DuckDB.")
				}
				messageBatch = nil // Clear the batch
				// Reset the batch timer for the next batch
				batchTimer.Reset(batchTimeout)
			}

			// Note: We are NOT explicitly committing Kafka offsets here.
			// The offset is tracked in DuckDB and used for seeking on startup.
			// If you enable automatic commits in ReaderConfig (CommitInterval),
			// Kafka will commit offsets periodically based on messages read,
			// but our primary offset tracking is in DuckDB.

			// --- End Process Received Message ---
		}
	}
	// --- End Message Consumption Loop ---
}

// --- DuckDB Helper Functions ---

// initializeDuckDB creates the necessary tables if they don't exist.
func initializeDuckDB(db *sql.DB) error {
	// Table to store raw P1 electricity data
	// Using VARCHAR for JSON value for flexibility. Can be parsed in DuckDB or dbt later.
	createElectricityTableSQL := `
	CREATE TABLE IF NOT EXISTS p1_electricity_raw (
		kafka_topic VARCHAR,
		kafka_partition INTEGER,
		kafka_offset BIGINT,
		kafka_timestamp TIMESTAMP, -- Kafka message timestamp
		processing_timestamp TIMESTAMP, -- Timestamp from the Go app
		telegram_timestamp VARCHAR, -- Original timestamp from P1 telegram
		raw_data VARCHAR -- Store raw JSON string
	);`

	// Table to store raw P1 gas data
	createGasTableSQL := `
	CREATE TABLE IF NOT EXISTS p1_gas_raw (
		kafka_topic VARCHAR,
		kafka_partition INTEGER,
		kafka_offset BIGINT,
		kafka_timestamp TIMESTAMP, -- Kafka message timestamp
		processing_timestamp TIMESTAMP, -- Timestamp from the Go app
		telegram_timestamp VARCHAR, -- Original timestamp from P1 telegram
		raw_data VARCHAR -- Store raw JSON string
	);`

	// Table to store the last processed Kafka offset for each topic/partition
	createOffsetsTableSQL := `
	CREATE TABLE IF NOT EXISTS kafka_offsets (
		consumer_group_id VARCHAR,
		topic VARCHAR,
		partition INTEGER,
		last_offset BIGINT,
		updated_at TIMESTAMP,
		PRIMARY KEY (consumer_group_id, topic, partition)
	);`

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for table initialization: %w", err)
	}
	defer tx.Rollback() // Rollback if anything goes wrong

	_, err = tx.Exec(createElectricityTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create p1_electricity_raw table: %w", err)
	}

	_, err = tx.Exec(createGasTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create p1_gas_raw table: %w", err)
	}

	_, err = tx.Exec(createOffsetsTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create kafka_offsets table: %w", err)
	}

	return tx.Commit() // Commit the transaction if all successful
}

// getLastProcessedOffsets retrieves the last committed offset for each topic/partition
// for the consumer group from the DuckDB kafka_offsets table.
// Returns a map [topic][partition] -> offset.
func getLastProcessedOffsets(db *sql.DB, topics []string) (map[string]map[int]int64, error) {
	offsets := make(map[string]map[int]int64)
	consumerGroupID := "p1-data-consumer-group" // Must match the GroupID in ReaderConfig

	// DuckDB doesn't have a direct way to query partitions for a topic without consuming.
	// We assume partitions are 0 for simplicity based on your Kafka setup.
	// In a multi-partition scenario, you'd need to discover partitions or configure them.
	partitions := []int{0} // Assuming partition 0 for all topics

	for _, topic := range topics {
		offsets[topic] = make(map[int]int64)
		for _, partition := range partitions {
			var lastOffset sql.NullInt64 // Use NullInt64 to handle cases where no offset exists yet
			query := `
			SELECT last_offset
			FROM kafka_offsets
			WHERE consumer_group_id = ? AND topic = ? AND partition = ?;`

			row := db.QueryRow(query, consumerGroupID, topic, partition)
			err := row.Scan(&lastOffset)

			if err == sql.ErrNoRows {
				// No offset found for this topic/partition/group, start from the beginning (-1)
				offsets[topic][partition] = kafka.FirstOffset // Use kafka.FirstOffset (-2) to start from the earliest available
				log.Printf("No previous offset found for topic '%s' partition %d. Will start from earliest available offset (%d).", topic, partition, kafka.FirstOffset)
			} else if err != nil {
				// Other database error
				return nil, fmt.Errorf("failed to query last offset for topic '%s' partition %d: %w", topic, partition, err)
			} else {
				// Offset found, use it. We will seek to offset + 1 in the main loop.
				offsets[topic][partition] = lastOffset.Int64
				log.Printf("Found last processed offset for topic '%s' partition %d: %d", topic, partition, lastOffset.Int64)
			}
		}
	}
	return offsets, nil
}

// writeBatchToDuckDB writes a batch of messages to the appropriate raw tables
// and updates the kafka_offsets table within a single transaction.
func writeBatchToDuckDB(db *sql.DB, messages []KafkaMessage) error {
	if len(messages) == 0 {
		return nil // Nothing to write
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for batch write: %w", err)
	}
	defer tx.Rollback() // Rollback if anything goes wrong

	// Prepare statements for inserting data and updating offsets
	// Using prepared statements can be more efficient for batch inserts
	insertElecStmt, err := tx.Prepare(`
		INSERT INTO p1_electricity_raw (kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, processing_timestamp, telegram_timestamp, raw_data)
		VALUES (?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return fmt.Errorf("failed to prepare electricity insert statement: %w", err)
	}
	defer insertElecStmt.Close()

	insertGasStmt, err := tx.Prepare(`
		INSERT INTO p1_gas_raw (kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, processing_timestamp, telegram_timestamp, raw_data)
		VALUES (?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return fmt.Errorf("failed to prepare gas insert statement: %w", err)
	}
	defer insertGasStmt.Close()

	updateOffsetStmt, err := tx.Prepare(`
		INSERT INTO kafka_offsets (consumer_group_id, topic, partition, last_offset, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (consumer_group_id, topic, partition)
		DO UPDATE SET last_offset = excluded.last_offset, updated_at = excluded.updated_at;`)
	if err != nil {
		return fmt.Errorf("failed to prepare offset update statement: %w", err)
	}
	defer updateOffsetStmt.Close()

	// Track the highest offset processed for each topic/partition in this batch
	highestOffsets := make(map[string]map[int]int64)
	consumerGroupID := "p1-data-consumer-group" // Must match

	// Process each message in the batch
	for _, msg := range messages {
		// Deserialize the JSON to extract the timestamps
		var data map[string]interface{}
		jsonErr := json.Unmarshal(msg.Value, &data)
		if jsonErr != nil {
			log.Printf("⚠️ Skipping message from topic %s partition %d offset %d due to JSON unmarshalling error: %v",
				msg.Topic, msg.Partition, msg.Offset, jsonErr)
			continue // Skip this message, but continue processing the batch
		}

		// Extract timestamps - handle potential missing fields gracefully
		telegramTimestamp, _ := data["telegramTimestamp"].(string) // Type assertion, ignore error

		// Insert into the appropriate raw table
		var insertErr error
		if msg.Topic == electricityTopic {
			// Note: We are storing the processingTimestampUTC from the JSON, not time.Now() here,
			// to use the timestamp that was actually put into the Kafka message.
			processingTimestampUTC, _ := data["processingTimestampUTC"].(string)                 // Assuming it's a string in ISO format
			parsedProcessingTimestamp, _ := time.Parse(time.RFC3339Nano, processingTimestampUTC) // Parse the string

			_, insertErr = insertElecStmt.Exec(
				msg.Topic,
				msg.Partition,
				msg.Offset,
				msg.Timestamp,             // Kafka message timestamp
				parsedProcessingTimestamp, // Processing timestamp from JSON
				telegramTimestamp,
				string(msg.Value), // Store raw JSON
			)
		} else if msg.Topic == gasTopic {
			// Note: We are storing the processingTimestampUTC from the JSON, not time.Now() here
			processingTimestampUTC, _ := data["processingTimestampUTC"].(string)                 // Assuming it's a string in ISO format
			parsedProcessingTimestamp, _ := time.Parse(time.RFC3339Nano, processingTimestampUTC) // Parse the string

			_, insertErr = insertGasStmt.Exec(
				msg.Topic,
				msg.Partition,
				msg.Offset,
				msg.Timestamp,             // Kafka message timestamp
				parsedProcessingTimestamp, // Processing timestamp from JSON
				telegramTimestamp,
				string(msg.Value), // Store raw JSON
			)
		} else {
			log.Printf("⚠️ Received message from unexpected topic '%s'. Skipping.", msg.Topic)
			continue // Skip this message
		}

		if insertErr != nil {
			// Log the error but continue processing the rest of the batch.
			// This message's offset will not be committed in DuckDB's offset table,
			// so it will be re-read on the next startup.
			log.Printf("❌ Failed to insert message from topic %s partition %d offset %d into DuckDB: %v",
				msg.Topic, msg.Partition, msg.Offset, insertErr)
			continue // Move to the next message in the batch
		}

		// Update the highest offset seen for this topic/partition in this batch
		if _, ok := highestOffsets[msg.Topic]; !ok {
			highestOffsets[msg.Topic] = make(map[int]int64)
		}
		currentHighest := highestOffsets[msg.Topic][msg.Partition]
		if msg.Offset > currentHighest {
			highestOffsets[msg.Topic][msg.Partition] = msg.Offset
		}
	}

	// Update the kafka_offsets table with the highest processed offset for each topic/partition
	for topic, partitions := range highestOffsets {
		for partition, offset := range partitions {
			_, err = updateOffsetStmt.Exec(
				consumerGroupID,
				topic,
				partition,
				offset,
				time.Now().UTC(),
			)
			if err != nil {
				// This is a critical error - we failed to commit the offset for a batch.
				// The transaction will be rolled back by the defer tx.Rollback().
				return fmt.Errorf("failed to update offset for topic '%s' partition %d: %w", topic, partition, err)
			}
		}
	}

	// Commit the transaction if all inserts and offset updates were successful
	return tx.Commit()
}

// getLastProcessedOffsets retrieves the last committed offset for each topic/partition
// for the consumer group from the DuckDB kafka_offsets table.
// Returns a map [topic][partition] -> offset.
func getLastProcessedOffsets(db *sql.DB, topics []string) (map[string]map[int]int64, error) {
	offsets := make(map[string]map[int]int64)
	consumerGroupID := "p1-data-consumer-group" // Must match the GroupID in ReaderConfig

	// DuckDB doesn't have a direct way to query partitions for a topic without consuming.
	// We assume partitions are 0 for simplicity based on your Kafka setup.
	// In a multi-partition scenario, you'd need to discover partitions or configure them.
	partitions := []int{0} // Assuming partition 0 for all topics

	for _, topic := range topics {
		offsets[topic] = make(map[int]int64)
		for _, partition := range partitions {
			var lastOffset sql.NullInt64 // Use NullInt64 to handle cases where no offset exists yet
			query := `
			SELECT last_offset
			FROM kafka_offsets
			WHERE consumer_group_id = ? AND topic = ? AND partition = ?;`

			row := db.QueryRow(query, consumerGroupID, topic, partition)
			err := row.Scan(&lastOffset)

			if err == sql.ErrNoRows {
				// No offset found for this topic/partition/group, start from the beginning (-1)
				offsets[topic][partition] = kafka.FirstOffset // Use kafka.FirstOffset (-2) to start from the earliest available
				log.Printf("No previous offset found for topic '%s' partition %d. Will start from earliest available offset (%d).", topic, partition, kafka.FirstOffset)
			} else if err != nil {
				// Other database error
				return nil, fmt.Errorf("failed to query last offset for topic '%s' partition %d: %w", topic, partition, err)
			} else {
				// Offset found, use it. We will seek to offset + 1 in the main loop.
				offsets[topic][partition] = lastOffset.Int64
				log.Printf("Found last processed offset for topic '%s' partition %d: %d", topic, partition, lastOffset.Int64)
			}
		}
	}
	return offsets, nil
}

// writeBatchToDuckDB writes a batch of messages to the appropriate raw tables
// and updates the kafka_offsets table within a single transaction.
func writeBatchToDuckDB(db *sql.DB, messages []KafkaMessage) error {
	if len(messages) == 0 {
		return nil // Nothing to write
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for batch write: %w", err)
	}
	defer tx.Rollback() // Rollback if anything goes wrong

	// Prepare statements for inserting data and updating offsets
	// Using prepared statements can be more efficient for batch inserts
	insertElecStmt, err := tx.Prepare(`
		INSERT INTO p1_electricity_raw (kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, processing_timestamp, telegram_timestamp, raw_data)
		VALUES (?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return fmt.Errorf("failed to prepare electricity insert statement: %w", err)
	}
	defer insertElecStmt.Close()

	insertGasStmt, err := tx.Prepare(`
		INSERT INTO p1_gas_raw (kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, processing_timestamp, telegram_timestamp, raw_data)
		VALUES (?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return fmt.Errorf("failed to prepare gas insert statement: %w", err)
	}
	defer insertGasStmt.Close()

	updateOffsetStmt, err := tx.Prepare(`
		INSERT INTO kafka_offsets (consumer_group_id, topic, partition, last_offset, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (consumer_group_id, topic, partition)
		DO UPDATE SET last_offset = excluded.last_offset, updated_at = excluded.updated_at;`)
	if err != nil {
		return fmt.Errorf("failed to prepare offset update statement: %w", err)
	}
	defer updateOffsetStmt.Close()

	// Track the highest offset processed for each topic/partition in this batch
	highestOffsets := make(map[string]map[int]int64)
	consumerGroupID := "p1-data-consumer-group" // Must match

	// Process each message in the batch
	for _, msg := range messages {
		// Deserialize the JSON to extract the timestamps
		var data map[string]interface{}
		jsonErr := json.Unmarshal(msg.Value, &data)
		if jsonErr != nil {
			log.Printf("⚠️ Skipping message from topic %s partition %d offset %d due to JSON unmarshalling error: %v",
				msg.Topic, msg.Partition, msg.Offset, jsonErr)
			continue // Skip this message, but continue processing the batch
		}

		// Extract timestamps - handle potential missing fields gracefully
		telegramTimestamp, _ := data["telegramTimestamp"].(string) // Type assertion, ignore error

		// Insert into the appropriate raw table
		var insertErr error
		if msg.Topic == electricityTopic {
			// Note: We are storing the processingTimestampUTC from the JSON, not time.Now() here,
			// to use the timestamp that was actually put into the Kafka message.
			processingTimestampUTC, _ := data["processingTimestampUTC"].(string)                 // Assuming it's a string in ISO format
			parsedProcessingTimestamp, _ := time.Parse(time.RFC3339Nano, processingTimestampUTC) // Parse the string

			_, insertErr = insertElecStmt.Exec(
				msg.Topic,
				msg.Partition,
				msg.Offset,
				msg.Timestamp,             // Kafka message timestamp
				parsedProcessingTimestamp, // Processing timestamp from JSON
				telegramTimestamp,
				string(msg.Value), // Store raw JSON
			)
		} else if msg.Topic == gasTopic {
			// Note: We are storing the processingTimestampUTC from the JSON, not time.Now() here
			processingTimestampUTC, _ := data["processingTimestampUTC"].(string)                 // Assuming it's a string in ISO format
			parsedProcessingTimestamp, _ := time.Parse(time.RFC3339Nano, processingTimestampUTC) // Parse the string

			_, insertErr = insertGasStmt.Exec(
				msg.Topic,
				msg.Partition,
				msg.Offset,
				msg.Timestamp,             // Kafka message timestamp
				parsedProcessingTimestamp, // Processing timestamp from JSON
				telegramTimestamp,
				string(msg.Value), // Store raw JSON
			)
		} else {
			log.Printf("⚠️ Received message from unexpected topic '%s'. Skipping.", msg.Topic)
			continue // Skip this message
		}

		if insertErr != nil {
			// Log the error but continue processing the rest of the batch.
			// This message's offset will not be committed in DuckDB's offset table,
			// so it will be re-read on the next startup.
			log.Printf("❌ Failed to insert message from topic %s partition %d offset %d into DuckDB: %v",
				msg.Topic, msg.Partition, msg.Offset, insertErr)
			continue // Move to the next message in the batch
		}

		// Update the highest offset seen for this topic/partition in this batch
		if _, ok := highestOffsets[msg.Topic]; !ok {
			highestOffsets[msg.Topic] = make(map[int]int64)
		}
		currentHighest := highestOffsets[msg.Topic][msg.Partition]
		if msg.Offset > currentHighest {
			highestOffsets[msg.Topic][msg.Partition] = msg.Offset
		}
	}

	// Update the kafka_offsets table with the highest processed offset for each topic/partition
	for topic, partitions := range highestOffsets {
		for partition, offset := range partitions {
			_, err = updateOffsetStmt.Exec(
				consumerGroupID,
				topic,
				partition,
				offset,
				time.Now().UTC(),
			)
			if err != nil {
				// This is a critical error - we failed to commit the offset for a batch.
				// The transaction will be rolled back by the defer tx.Rollback().
				return fmt.Errorf("failed to update offset for topic '%s' partition %d: %w", topic, partition, err)
			}
		}
	}

	// Commit the transaction if all inserts and offset updates were successful
	return tx.Commit()
}

// getLastProcessedOffsets retrieves the last committed offset for each topic/partition
// for the consumer group from the DuckDB kafka_offsets table.
// Returns a map [topic][partition] -> offset.
func getLastProcessedOffsets(db *sql.DB, topics []string) (map[string]map[int]int64, error) {
	offsets := make(map[string]map[int]int64)
	consumerGroupID := "p1-data-consumer-group" // Must match the GroupID in ReaderConfig

	// DuckDB doesn't have a direct way to query partitions for a topic without consuming.
	// We assume partitions are 0 for simplicity based on your Kafka setup.
	// In a multi-partition scenario, you'd need to discover partitions or configure them.
	partitions := []int{0} // Assuming partition 0 for all topics

	for _, topic := range topics {
		offsets[topic] = make(map[int]int64)
		for _, partition := range partitions {
			var lastOffset sql.NullInt64 // Use NullInt64 to handle cases where no offset exists yet
			query := `
			SELECT last_offset
			FROM kafka_offsets
			WHERE consumer_group_id = ? AND topic = ? AND partition = ?;`

			row := db.QueryRow(query, consumerGroupID, topic, partition)
			err := row.Scan(&lastOffset)

			if err == sql.ErrNoRows {
				// No offset found for this topic/partition/group, start from the beginning (-1)
				offsets[topic][partition] = kafka.FirstOffset // Use kafka.FirstOffset (-2) to start from the earliest available
				log.Printf("No previous offset found for topic '%s' partition %d. Will start from earliest available offset (%d).", topic, partition, kafka.FirstOffset)
			} else if err != nil {
				// Other database error
				return nil, fmt.Errorf("failed to query last offset for topic '%s' partition %d: %w", topic, partition, err)
			} else {
				// Offset found, use it. We will seek to offset + 1 in the main loop.
				offsets[topic][partition] = lastOffset.Int64
				log.Printf("Found last processed offset for topic '%s' partition %d: %d", topic, partition, lastOffset.Int64)
			}
		}
	}
	return offsets, nil
}

// writeBatchToDuckDB writes a batch of messages to the appropriate raw tables
// and updates the kafka_offsets table within a single transaction.
func writeBatchToDuckDB(db *sql.DB, messages []KafkaMessage) error {
	if len(messages) == 0 {
		return nil // Nothing to write
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for batch write: %w", err)
	}
	defer tx.Rollback() // Rollback if anything goes wrong

	// Prepare statements for inserting data and updating offsets
	// Using prepared statements can be more efficient for batch inserts
	insertElecStmt, err := tx.Prepare(`
		INSERT INTO p1_electricity_raw (kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, processing_timestamp, telegram_timestamp, raw_data)
		VALUES (?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return fmt.Errorf("failed to prepare electricity insert statement: %w", err)
	}
	defer insertElecStmt.Close()

	insertGasStmt, err := tx.Prepare(`
		INSERT INTO p1_gas_raw (kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, processing_timestamp, telegram_timestamp, raw_data)
		VALUES (?, ?, ?, ?, ?, ?, ?);`)
	if err != nil {
		return fmt.Errorf("failed to prepare gas insert statement: %w", err)
	}
	defer insertGasStmt.Close()

	updateOffsetStmt, err := tx.Prepare(`
		INSERT INTO kafka_offsets (consumer_group_id, topic, partition, last_offset, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (consumer_group_id, topic, partition)
		DO UPDATE SET last_offset = excluded.last_offset, updated_at = excluded.updated_at;`)
	if err != nil {
		return fmt.Errorf("failed to prepare offset update statement: %w", err)
	}
	defer updateOffsetStmt.Close()

	// Track the highest offset processed for each topic/partition in this batch
	highestOffsets := make(map[string]map[int]int64)
	consumerGroupID := "p1-data-consumer-group" // Must match

	// Process each message in the batch
	for _, msg := range messages {
		// Deserialize the JSON to extract the timestamps
		var data map[string]interface{}
		jsonErr := json.Unmarshal(msg.Value, &data)
		if jsonErr != nil {
			log.Printf("⚠️ Skipping message from topic %s partition %d offset %d due to JSON unmarshalling error: %v",
				msg.Topic, msg.Partition, msg.Offset, jsonErr)
			continue // Skip this message, but continue processing the batch
		}

		// Extract timestamps - handle potential missing fields gracefully
		telegramTimestamp, _ := data["telegramTimestamp"].(string) // Type assertion, ignore error

		// Insert into the appropriate raw table
		var insertErr error
		if msg.Topic == electricityTopic {
			// Note: We are storing the processingTimestampUTC from the JSON, not time.Now() here,
			// to use the timestamp that was actually put into the Kafka message.
			processingTimestampUTC, _ := data["processingTimestampUTC"].(string)                 // Assuming it's a string in ISO format
			parsedProcessingTimestamp, _ := time.Parse(time.RFC3339Nano, processingTimestampUTC) // Parse the string

			_, insertErr = insertElecStmt.Exec(
				msg.Topic,
				msg.Partition,
				msg.Offset,
				msg.Timestamp,             // Kafka message timestamp
				parsedProcessingTimestamp, // Processing timestamp from JSON
				telegramTimestamp,
				string(msg.Value), // Store raw JSON
			)
		} else if msg.Topic == gasTopic {
			// Note: We are storing the processingTimestampUTC from the JSON, not time.Now() here
			processingTimestampUTC, _ := data["processingTimestampUTC"].(string)                 // Assuming it's a string in ISO format
			parsedProcessingTimestamp, _ := time.Parse(time.RFC3339Nano, processingTimestampUTC) // Parse the string

			_, insertErr = insertGasStmt.Exec(
				msg.Topic,
				msg.Partition,
				msg.Offset,
				msg.Timestamp,             // Kafka message timestamp
				parsedProcessingTimestamp, // Processing timestamp from JSON
				telegramTimestamp,
				string(msg.Value), // Store raw JSON
			)
		} else {
			log.Printf("⚠️ Received message from unexpected topic '%s'. Skipping.", msg.Topic)
			continue // Skip this message
		}

		if insertErr != nil {
			// Log the error but continue processing the rest of the batch.
			// This message's offset will not be committed in DuckDB's offset table,
			// so it will be re-read on the next startup.
			log.Printf("❌ Failed to insert message from topic %s partition %d offset %d into DuckDB: %v",
				msg.Topic, msg.Partition, msg.Offset, insertErr)
			continue // Move to the next message in the batch
		}

		// Update the highest offset seen for this topic/partition in this batch
		if _, ok := highestOffsets[msg.Topic]; !ok {
			highestOffsets[msg.Topic] = make(map[int]int64)
		}
		currentHighest := highestOffsets[msg.Topic][msg.Partition]
		if msg.Offset > currentHighest {
			highestOffsets[msg.Topic][msg.Partition] = msg.Offset
		}
	}

	// Update the kafka_offsets table with the highest processed offset for each topic/partition
	for topic, partitions := range highestOffsets {
		for partition, offset := range partitions {
			_, err = updateOffsetStmt.Exec(
				consumerGroupID,
				topic,
				partition,
				offset,
				time.Now().UTC(),
			)
			if err != nil {
				// This is a critical error - we failed to commit the offset for a batch.
				// The transaction will be rolled back by the defer tx.Rollback().
				return fmt.Errorf("failed to update offset for topic '%s' partition %d: %w", topic, partition, err)
			}
		}
	}

	// Commit the transaction if all inserts and offset updates were successful
	return tx.Commit()
}

// getLastProcessedOffsets retrieves the last committed offset for each topic/partition
// for the consumer group from the DuckDB kafka_offsets table.
// Returns a map [topic][partition] -> offset.
func getLastProcessedOffsets(db *sql.DB, topics []string) (map[string]map[int]int64, error) {
	offsets := make(map[string]map[int]int64)
	consumerGroupID := "p1-data-consumer-group" // Must match the GroupID in ReaderConfig

	// DuckDB doesn't have a direct way to query partitions for a topic without consuming.
	// We assume partitions are 0 for simplicity based on your Kafka setup.
	// In a multi-partition scenario, you'd need to discover partitions or configure them.
	partitions := []int{0} // Assuming partition 0 for all topics

	for _, topic := range topics {
		offsets[topic] = make(map[int]int64)
		for _, partition := range partitions {
			var lastOffset sql.NullInt64 // Use NullInt64 to handle cases where no offset exists yet
			query := `
			SELECT last_offset
			FROM kafka_offsets
			WHERE consumer_group_id = ? AND topic = ? AND partition = ?;`

			row := db.QueryRow(query, consumerGroupID, topic, partition)
			err := row.Scan(&lastOffset)

			if err == sql.ErrNoRows {
				// No offset found for this topic/partition/group, start from the beginning (-1)
				offsets[topic][partition] = kafka.FirstOffset // Use kafka.FirstOffset (-2) to start from the earliest available
				log.Printf("No previous offset found for topic '%s' partition %d. Will start from earliest available offset (%d).", topic, partition, kafka.FirstOffset)
			} else if err != nil {
				// Other database error
				return nil, fmt.Errorf("failed to query last offset for topic '%s' partition %d: %w", topic, partition, err)
			} else {
				// Offset found, use it. We will seek to offset + 1 in the main loop.
				offsets[topic][partition] = lastOffset.Int64
				log.Printf("Found last processed offset for topic '%s' partition %d: %d", topic, partition, lastOffset.Int64)
			}
		}
	}
	return offsets, nil
}
