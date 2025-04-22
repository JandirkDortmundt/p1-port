package main

import (
	"bufio"         // Used for efficient line-by-line reading
	"context"       // For Kafka producer context
	"encoding/json" // For JSON serialization
	"fmt"           // For printing output
	"io"            // For checking EOF error
	"log"           // For logging status messages and errors
	"os"            // For signal handling and error checking (ErrDeadlineExceeded)
	"os/signal"     // For signal handling
	"regexp"        // For regular expression matching to extract values
	"strconv"       // For converting extracted strings to numbers
	"strings"       // For string manipulation (trimming, splitting)
	"syscall"       // For signal handling (SIGTERM)
	"time"          // Used for potential delays

	// Third-party libraries
	"github.com/joho/godotenv"      // For loading environment variables from .env
	"github.com/segmentio/kafka-go" // For Kafka producer
	"go.bug.st/serial"              // For serial communication
)

// --- Configuration ---
// !! IMPORTANT: Replace "/dev/ttyUSB0" with the actual serial port name for your P1 cable on your Raspberry Pi !!
// This will ideally be read from the environment/config later.
const portName = "/dev/ttyUSB0" // Example: "/dev/ttyUSB0", "/dev/ttyACM0"

// Common P1 port settings (DSMR v5.0 standard).
const (
	baudRate = 115200
	dataBits = 8
	parity   = serial.NoParity
	stopBits = serial.OneStopBit
)

// Interval between the *end* of one successful read cycle and the *start* of the next.
// Set to slightly less than the other app's interval (e.g., 9 seconds if the other app reads every 10s)
// to ensure you get a chance to read, but adjust as needed based on actual timing.
const readInterval = 9 * time.Second

// --- Data Structures ---

// P1Data holds the extracted values from a single telegram.
// JSON tags with "omitempty" are used to control which fields are included
// in the JSON output when marshalling a map containing these values.
type P1Data struct {
	Timestamp               string  `json:"timestamp"`                         // Telegram timestamp (YYMMDDHHMMSS) - Always included
	ElectricityConsumption1 float64 `json:"electricityConsumption1,omitempty"` // Meter Reading electricity delivered to client (Tariff 1) in kWh
	ElectricityConsumption2 float64 `json:"electricityConsumption2,omitempty"` // Meter Reading electricity delivered to client (Tariff 2) in kWh
	ElectricityProduction1  float64 `json:"electricityProduction1,omitempty"`  // Meter Reading electricity delivered by client (Tariff 1) in kWh
	ElectricityProduction2  float64 `json:"electricityProduction2,omitempty"`  // Meter Reading electricity delivered by client (Tariff 2) in kWh
	CurrentTariff           int     `json:"currentTariff,omitempty"`           // Current Tariff indicator (1 or 2)
	CurrentConsumption      float64 `json:"currentConsumption,omitempty"`      // Actual electricity power delivered (+P) in kW
	CurrentProduction       float64 `json:"currentProduction,omitempty"`       // Actual electricity power received (-P) in kW
	GasTimestamp            string  `json:"gasTimestamp,omitempty"`            // Timestamp of gas reading (YYMMDDHHMMSS)
	GasConsumption          float64 `json:"gasConsumption,omitempty"`          // Gas meter reading in m3
}

// --- Regular Expressions ---
// Pre-compile regexps for efficiency. These target specific OBIS codes and capture the value(s).
var (
	regexTimestamp      = regexp.MustCompile(`^0-0:1\.0\.0\(([0-9]{12})[WS]\)`)
	regexElecConsume1   = regexp.MustCompile(`^1-0:1\.8\.1\(([0-9]+\.[0-9]{3})\*kWh\)`)
	regexElecConsume2   = regexp.MustCompile(`^1-0:1\.8\.2\(([0-9]+\.[0-9]{3})\*kWh\)`)
	regexElecProduce1   = regexp.MustCompile(`^1-0:2\.8\.1\(([0-9]+\.[0-9]{3})\*kWh\)`)
	regexElecProduce2   = regexp.MustCompile(`^1-0:2\.8\.2\(([0-9]+\.[0-9]{3})\*kWh\)`)
	regexTariff         = regexp.MustCompile(`^0-0:96\.14\.0\(([0-9]{4})\)`)
	regexCurrentConsume = regexp.MustCompile(`^1-0:1\.7\.0\(([0-9]+\.[0-9]{3})\*kW\)`)
	regexCurrentProduce = regexp.MustCompile(`^1-0:2\.7\.0\(([0-9]+\.[0-9]{3})\*kW\)`)
	regexGas            = regexp.MustCompile(`^0-1:24\.2\.1\(([0-9]{12})[WS]\)\(([0-9]+\.[0-9]{3})\*m3\)`)
)

// --- Global Variables ---
var lastParsedData P1Data // Stores the data from the previous successful telegram parse

// --- Main Function ---

func main() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		// Log a warning if .env is not found, but don't fatal,
		// as variables might be set directly in the environment.
		log.Println("Warning: Could not load .env file. Ensure environment variables are set.")
	}

	// Read Kafka configuration from environment variables
	kafkaBroker := os.Getenv("KAFKA_BROKER_ADDRESS")
	electricityTopic := os.Getenv("KAFKA_ELECTRICITY_TOPIC")
	gasTopic := os.Getenv("KAFKA_GAS_TOPIC")

	if kafkaBroker == "" || electricityTopic == "" || gasTopic == "" {
		log.Fatal("Kafka environment variables (KAFKA_BROKER_ADDRESS, KAFKA_ELECTRICITY_TOPIC, KAFKA_GAS_TOPIC) not set!")
	}

	log.Printf("Kafka Broker Address: %s", kafkaBroker)
	log.Printf("Electricity Topic: %s", electricityTopic)
	log.Printf("Gas Topic: %s", gasTopic)
	log.Println("Starting P1 telegram reader and Kafka producer...")

	// --- Kafka Producer Setup ---
	// Create a new Kafka producer
	producer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Balancer: &kafka.LeastBytes{}, // Use LeastBytes balancer for even distribution
		// Tune batching for better performance
		BatchTimeout: 1 * time.Second,                             // Wait up to 1 second to build a batch (decreased from 5s)
		BatchSize:    200,                                         // Attempt to batch up to 200 messages (increased from 100)
		RequiredAcks: kafka.RequireAll,                            // Keep RequireAll for durability (can change to RequiredLocal if needed for speed)
		Logger:       log.New(os.Stdout, "KAFKA ", log.LstdFlags), // Add Kafka-specific logging
		// WriteTimeout is set per WriteMessages call via context
	}
	// Ensure the producer is closed when the main function exits
	defer func() {
		log.Println("Closing Kafka producer...")
		if closeErr := producer.Close(); closeErr != nil {
			log.Printf("Error closing Kafka producer: %v", closeErr)
		} else {
			log.Println("Kafka producer closed successfully.")
		}
	}()
	// --- End Kafka Producer Setup ---

	// --- Signal Handling ---
	// Create a channel to listen for OS signals
	sigChan := make(chan os.Signal, 1)
	// Notify the channel for Interrupt (Ctrl+C) and Terminate signals
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create a context that will be cancelled on shutdown signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancel is called

	// Start a goroutine that waits for a signal and cancels the context
	go func() {
		<-sigChan // Block until a signal is received
		log.Println("Received interrupt signal. Initiating shutdown...")
		cancel() // Cancel the context
	}()
	// --- End Signal Handling ---

	// Main loop to repeatedly open, read, close, and wait
	for {
		select {
		case <-ctx.Done(): // Check if context is cancelled (shutdown signal)
			log.Println("Shutdown signal received. Exiting program.")
			return // Exit main function
		default:
			// Continue with the read cycle
		}

		log.Printf("Attempting to open serial port %s...", portName)

		mode := &serial.Mode{
			BaudRate: baudRate,
			DataBits: dataBits,
			Parity:   parity,
			StopBits: stopBits,
		}

		port, err := serial.Open(portName, mode)
		if err != nil {
			log.Printf("❌ Failed to open port %s: %v\n"+
				"   Hints:\n"+
				"   1. Is the device name '%s' correct? (Check with 'ls /dev/tty*')\n"+
				"   2. Is the P1 cable plugged in?\n"+
				"   3. Does the user have permission? Try 'sudo usermod -a -G dialout $USER' (and relogin).\n"+
				"   4. Is another program using the port? (Use 'lsof %s' or 'fuser %s')",
				portName, err, portName, portName, portName)
			// If we can't open the port, wait before trying again to avoid spamming
			select {
			case <-time.After(readInterval):
				// Waited for the interval, try again
				continue
			case <-ctx.Done(): // Check if context is cancelled while waiting
				log.Println("Shutdown signal received while waiting to open port. Exiting program.")
				return
			}
		}

		log.Printf("✅ Successfully opened serial port %s. Reading telegram...", portName)

		reader := bufio.NewReader(port)
		var telegramLines []string // Buffer to hold lines of the current telegram
		readingTelegram := false   // Flag to indicate if we are currently inside a telegram
		telegramComplete := false

		// Loop to read lines until a complete telegram is found or shutdown is signaled
		for !telegramComplete {
			select {
			case <-ctx.Done(): // Check if context is cancelled (shutdown signal)
				log.Println("Shutdown signal received while reading telegram. Closing port.")
				port.Close() // Close the port immediately
				return       // Exit main function
			default:
				// Continue reading line
			}

			// Set a read deadline to prevent blocking forever if data stops flowing
			// This is important in an intermittent read scenario.
			// Adjust timeout based on how quickly your meter sends data (usually within a few seconds)
			port.SetReadTimeout(5 * time.Second)

			line, err := reader.ReadString('\n')

			if err != nil {
				if err == io.EOF {
					log.Println("Received EOF while reading telegram. Port likely closed unexpectedly.")
				} else if os.IsTimeout(err) {
					log.Printf("⚠️ Read timeout after 5 seconds while reading telegram. Port may be busy or no data received.")
				} else {
					log.Printf("⚠️ Error reading line from serial port: %v", err)
				}
				// On any read error during telegram assembly, abandon this telegram
				// and close the port to reset.
				port.Close()
				telegramLines = nil     // Clear buffer
				readingTelegram = false // Reset state
				break                   // Exit the inner read loop to try opening the port again
			}

			// Trim whitespace (like CR/LF)
			trimmedLine := strings.TrimSpace(line)

			// --- Telegram Synchronization Logic ---
			if !readingTelegram {
				// We are looking for the start of a new telegram
				if strings.HasPrefix(trimmedLine, "/") {
					log.Println("Found start of telegram ('/'). Beginning to collect lines.")
					readingTelegram = true
					telegramLines = []string{trimmedLine} // Start collecting with the '/' line
				} else {
					// Ignore lines until the start marker is found
					// log.Printf("Ignoring line outside of telegram: %s", trimmedLine) // Optional: uncomment for debugging ignored lines
				}
			} else {
				// We are currently reading inside a telegram
				// Add the line to our buffer if not empty
				if len(trimmedLine) > 0 {
					telegramLines = append(telegramLines, trimmedLine)
				}

				// Check if this line is the end-of-telegram marker '!'
				if strings.HasPrefix(trimmedLine, "!") {
					log.Println("Found end of telegram ('!'). Telegram complete.")
					telegramComplete = true // Mark telegram as complete
				}
			}
			// --- End Synchronization Logic ---
		}

		// --- Process Telegram (if complete) ---
		if telegramComplete && len(telegramLines) > 0 {
			log.Println("Telegram complete. Parsing...")
			parsedData, err := parseTelegram(telegramLines)
			if err != nil {
				log.Printf("⚠️ Error parsing telegram: %v", err)
			} else {
				// Print the parsed data nicely
				fmt.Println("--- Parsed P1 Data ---")
				fmt.Printf("  Timestamp: %s\n", parsedData.Timestamp)
				fmt.Printf("  Electricity Tariff: %d\n", parsedData.CurrentTariff)
				fmt.Printf("  Consumption (T1): %.3f kWh\n", parsedData.ElectricityConsumption1)
				fmt.Printf("  Consumption (T2): %.3f kWh\n", parsedData.ElectricityConsumption2)
				fmt.Printf("  Production  (T1): %.3f kWh\n", parsedData.ElectricityProduction1)
				fmt.Printf("  Production  (T2): %.3f kWh\n", parsedData.ElectricityProduction2)
				fmt.Printf("  Current Consumption: %.3f kW\n", parsedData.CurrentConsumption)
				fmt.Printf("  Current Production:  %.3f kW\n", parsedData.CurrentProduction)
				fmt.Printf("  Gas Timestamp: %s\n", parsedData.GasTimestamp)
				fmt.Printf("  Gas Consumption: %.3f m3\n", parsedData.GasConsumption)
				fmt.Println("----------------------")

				// --- Kafka Producing Logic (Send only changed values) ---
				// Build maps containing only the fields that have changed, plus the timestamps.
				// This results in a JSON object where missing fields indicate no change.
				electricityUpdate := make(map[string]interface{})
				gasUpdate := make(map[string]interface{})

				// Flag to track if any non-timestamp fields changed
				electricityChanged := false
				gasChanged := false

				// Always include the P1 telegram timestamp and the processing timestamp for context
				electricityUpdate["telegramTimestamp"] = parsedData.Timestamp
				gasUpdate["telegramTimestamp"] = parsedData.Timestamp
				// Add the current UTC time when this data is being processed
				processingTimestamp := time.Now().UTC()
				electricityUpdate["processingTimestampUTC"] = processingTimestamp
				gasUpdate["processingTimestampUTC"] = processingTimestamp

				// Compare and add changed electricity fields
				if parsedData.ElectricityConsumption1 != lastParsedData.ElectricityConsumption1 {
					electricityUpdate["electricityConsumption1"] = parsedData.ElectricityConsumption1
					electricityChanged = true
				}
				if parsedData.ElectricityConsumption2 != lastParsedData.ElectricityConsumption2 {
					electricityUpdate["electricityConsumption2"] = parsedData.ElectricityConsumption2
					electricityChanged = true
				}
				if parsedData.ElectricityProduction1 != lastParsedData.ElectricityProduction1 {
					electricityUpdate["electricityProduction1"] = parsedData.ElectricityProduction1
					electricityChanged = true
				}
				if parsedData.ElectricityProduction2 != lastParsedData.ElectricityProduction2 {
					electricityUpdate["electricityProduction2"] = parsedData.ElectricityProduction2
					electricityChanged = true
				}
				if parsedData.CurrentTariff != lastParsedData.CurrentTariff {
					electricityUpdate["currentTariff"] = parsedData.CurrentTariff
					electricityChanged = true
				}
				if parsedData.CurrentConsumption != lastParsedData.CurrentConsumption {
					electricityUpdate["currentConsumption"] = parsedData.CurrentConsumption
					electricityChanged = true
				}
				if parsedData.CurrentProduction != lastParsedData.CurrentProduction {
					electricityUpdate["currentProduction"] = parsedData.CurrentProduction
					electricityChanged = true
				}

				// Compare and add changed gas fields
				// Note: Gas timestamp is also a value from the meter, compare it
				if parsedData.GasTimestamp != lastParsedData.GasTimestamp {
					gasUpdate["gasTimestamp"] = parsedData.GasTimestamp // Include the meter's gas timestamp if it changed
					gasChanged = true
				}
				if parsedData.GasConsumption != lastParsedData.GasConsumption {
					gasUpdate["gasConsumption"] = parsedData.GasConsumption
					gasChanged = true
				}

				// Send electricity update if any electricity-related values changed
				// We also send if it's the very first read (lastParsedData is zero-valued)
				if electricityChanged || (lastParsedData == P1Data{}) {
					msgValue, jsonErr := json.Marshal(electricityUpdate)
					if jsonErr != nil {
						log.Printf("⚠️ Error marshalling electricity data to JSON: %v", jsonErr)
					} else {
						msg := kafka.Message{
							Topic: electricityTopic,
							Value: msgValue,
							// Consider adding a Key if you want messages for the same meter
							// to go to the same partition. For P1 data, the meter is the source,
							// so a fixed key representing this meter might be appropriate.
							// Key: []byte("your-meter-id"), // Example
							Time: processingTimestamp, // Set Kafka message timestamp to current UTC time
						}
						// Increased timeout for writing to Kafka
						writeCtx, cancelWrite := context.WithTimeout(ctx, 10*time.Second)
						defer cancelWrite()
						log.Printf("KAFKA: Attempting to write electricity message to topic %s...", electricityTopic)
						err := producer.WriteMessages(writeCtx, msg)
						if err != nil {
							log.Printf("❌ Failed to write electricity message to Kafka: %v", err)
						} else {
							log.Printf("✅ Successfully wrote electricity message to topic %s", electricityTopic)
							// log.Printf("✅ Sent electricity update to topic %s: %s", electricityTopic, string(msgValue)) // Uncomment for verbose output
						}
					}
				} else {
					log.Println("No electricity values changed. Skipping Kafka message.")
				}

				// Send gas update if any gas-related values changed
				// We also send if it's the very first read (lastParsedData is zero-valued)
				if gasChanged || (lastParsedData == P1Data{}) {
					msgValue, jsonErr := json.Marshal(gasUpdate)
					if jsonErr != nil {
						log.Printf("⚠️ Error marshalling gas data to JSON: %v", jsonErr)
					} else {
						msg := kafka.Message{
							Topic: gasTopic,
							Value: msgValue,
							// Consider adding a Key here as well, perhaps the same meter ID
							// Key: []byte("your-meter-id"), // Example
							Time: processingTimestamp, // Set Kafka message timestamp to current UTC time
						}
						// Increased timeout for writing to Kafka
						writeCtx, cancelWrite := context.WithTimeout(ctx, 10*time.Second)
						defer cancelWrite()
						log.Printf("KAFKA: Attempting to write gas message to topic %s...", gasTopic)
						err := producer.WriteMessages(writeCtx, msg)
						if err != nil {
							log.Printf("❌ Failed to write gas message to Kafka: %v", err)
						} else {
							log.Printf("✅ Successfully wrote gas message to topic %s", gasTopic)
							// log.Printf("✅ Sent gas update to topic %s: %s", gasTopic, string(msgValue)) // Uncomment for verbose output
						}
					}
				} else {
					log.Println("No gas values changed. Skipping Kafka message.")
				}

				// Update lastParsedData for the next comparison
				// Note: On the very first read, lastParsedData will be the zero value,
				// causing all fields to be considered "changed" and sent. This is desired
				// to establish the initial state in Kafka.
				lastParsedData = parsedData

				// --- End Kafka Producing Logic ---
			}
		} else if !telegramComplete {
			// If the inner loop broke without completing a telegram (due to read error/timeout)
			log.Println("Telegram read incomplete due to error or timeout.")
		}
		// --- End Process Telegram ---

		// --- Close Port and Wait ---
		log.Println("Closing serial port...")
		if closeErr := port.Close(); closeErr != nil {
			log.Printf("Error closing serial port: %v", closeErr)
		} else {
			log.Println("Serial port closed successfully.")
		}

		log.Printf("Waiting for %s before next read...", readInterval)
		select {
		case <-time.After(readInterval):
			// Waited for the interval, continue loop for next read cycle
		case <-ctx.Done(): // Check if context is cancelled while waiting
			log.Println("Shutdown signal received while waiting. Exiting program.")
			return // Exit main function
		}
		// --- End Close Port and Wait ---
	}
}

// --- Helper Functions ---

// parseTelegram takes a slice of strings (lines of a P1 telegram)
// and attempts to extract relevant data using regular expressions.
func parseTelegram(lines []string) (P1Data, error) {
	data := P1Data{}
	var parseErrors []string // Collect errors encountered during parsing

	for _, line := range lines {
		// Try matching each line against our pre-compiled regexps
		if match := regexTimestamp.FindStringSubmatch(line); len(match) > 1 {
			data.Timestamp = match[1]
		} else if match := regexElecConsume1.FindStringSubmatch(line); len(match) > 1 {
			// Use blank identifier _ to ignore potential ParseFloat error for brevity
			data.ElectricityConsumption1, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexElecConsume2.FindStringSubmatch(line); len(match) > 1 {
			data.ElectricityConsumption2, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexElecProduce1.FindStringSubmatch(line); len(match) > 1 {
			data.ElectricityProduction1, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexElecProduce2.FindStringSubmatch(line); len(match) > 1 {
			data.ElectricityProduction2, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexTariff.FindStringSubmatch(line); len(match) > 1 {
			// Use blank identifier _ to ignore potential Atoi error for brevity
			data.CurrentTariff, _ = strconv.Atoi(match[1])
		} else if match := regexCurrentConsume.FindStringSubmatch(line); len(match) > 1 {
			data.CurrentConsumption, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexCurrentProduce.FindStringSubmatch(line); len(match) > 1 {
			data.CurrentProduction, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexGas.FindStringSubmatch(line); len(match) > 2 { // Expecting 2 capture groups
			data.GasTimestamp = match[1]
			data.GasConsumption, _ = strconv.ParseFloat(match[2], 64)
		}
		// Add more 'else if' blocks here for other OBIS codes you want to parse
	}

	// Basic validation: Check if at least the main timestamp was found
	if data.Timestamp == "" {
		parseErrors = append(parseErrors, "main timestamp (0-0:1.0.0) not found")
	}
	// You could add more checks here (e.g., was gas data found?)

	if len(parseErrors) > 0 {
		return data, fmt.Errorf("parsing failed for fields: %s", strings.Join(parseErrors, ", "))
	}

	return data, nil
}
