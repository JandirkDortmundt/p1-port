import os
import json
import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone

# Third-party libraries
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog, load_catalog

# Import PartitionSpec and PartitionField if you plan to use partitioning
# from pyiceberg.partitioning import PartitionSpec, PartitionField


# --- Configuration ---
# Load environment variables from .env file
load_dotenv()

KAFKA_BROKER_ADDRESS = os.getenv("KAFKA_BROKER_ADDRESS")
ELECTRICITY_TOPIC = os.getenv(
    "KAFKA_ELECTRICITY_TOPIC"
)  # Corrected topic name variable
GAS_TOPIC = os.getenv("KAFKA_GAS_TOPIC")  # Corrected topic name variable
# Corrected: Use the environment variable name from the user's .env file
DUCKDB_OFFSET_PATH = os.getenv(
    "DUCKDB_OFFSET_PATH"
)  # Root directory for DuckDB offsets DB
# ICEBERG_LAKEHOUSE_PATH is still used for logging and ensuring the directory exists,
# but pyiceberg loads the actual warehouse path from the ~/.pyiceberg.yaml config.
ICEBERG_LAKEHOUSE_PATH = os.getenv(
    "ICEBERG_LAKEHOUSE_PATH"
)  # Root directory for Iceberg catalog and data


# --- Explicit Configuration Check ---
# Perform checks immediately after loading environment variables
if not KAFKA_BROKER_ADDRESS:
    logging.error("❌ KAFKA_BROKER_ADDRESS environment variable not set!")
    sys.exit(1)
if not ELECTRICITY_TOPIC:
    logging.error("❌ ELECTRICITY_TOPIC environment variable not set!")
    sys.exit(1)
if not GAS_TOPIC:
    logging.error("❌ GAS_TOPIC environment variable not set!")
    sys.exit(1)
# Corrected: Check the variable name used in the .env file
if not DUCKDB_OFFSET_PATH:
    # This is the variable causing the current error
    logging.error("❌ DUCKDB_OFFSET_PATH environment variable not set!")
    sys.exit(1)
if not ICEBERG_LAKEHOUSE_PATH:
    logging.error("❌ ICEBERG_LAKEHOUSE_PATH environment variable not set!")
    sys.exit(1)
# --- End Explicit Configuration Check ---


# Iceberg table names (should match the tables created by setup_iceberg.py)
ELECTRICITY_ICEBERG_TABLE = "raw_p1_electricity_iceberg"
GAS_ICEBERG_TABLE = "raw_p1_gas_iceberg"

# Consumer group ID - MUST be unique for this consumer group
CONSUMER_GROUP_ID = "p1-data-consumer-group"

# Batching configuration for writing to Iceberg
BATCH_SIZE = 100  # Number of messages to collect before writing a batch
BATCH_TIMEOUT_SECONDS = (
    5  # Maximum time to wait before writing a batch, even if BatchSize is not reached
)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("p1_iceberg_consumer")

# --- Global Variables ---
shutdown_flag = threading.Event()  # Event to signal shutdown across threads


# --- Signal Handling ---
def signal_handler(signum, frame):
    """Handles OS signals (like Ctrl+C) for graceful shutdown."""
    logger.info("Received signal %s. Initiating graceful shutdown...", signum)
    shutdown_flag.set()  # Set the shutdown event


# Register the signal handler for SIGINT (Ctrl+C) and SIGTERM
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# --- DuckDB Offset Tracking Functions ---


def initialize_duckdb_offsets(db_path: str):
    """Initializes the DuckDB database and kafka_offsets table if they don't exist."""
    try:
        # Check if db_path is valid before connecting - Redundant now with top-level check, but safe.
        # if not isinstance(db_path, (str, os.PathLike)):
        #      raise TypeError(f"Invalid database path type: {type(db_path)}. Expected str or PathLike.")

        con = duckdb.connect(database=db_path)
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS kafka_offsets (
                consumer_group_id VARCHAR,
                topic VARCHAR,
                partition INTEGER,
                last_offset BIGINT,
                updated_at TIMESTAMP,
                PRIMARY KEY (consumer_group_id, topic, partition)
            );
        """
        )
        con.close()
        logger.info("✅ DuckDB offsets table initialized at %s", db_path)
    except Exception as e:
        logger.error("❌ Failed to initialize DuckDB offsets table: %s", e)
        sys.exit(1)  # Exit if we can't initialize the offsets DB


def get_last_processed_offsets(
    db_path: str, topics: list[str]
) -> dict[TopicPartition, int]:
    """Retrieves the last processed offset for each topic/partition from DuckDB."""
    offsets = {}
    try:
        # Check if db_path is valid before connecting - Redundant now with top-level check, but safe.
        # if not isinstance(db_path, (str, os.PathLike)):
        #      raise TypeError(f"Invalid database path type: {type(db_path)}. Expected str or PathLike.")

        con = duckdb.connect(database=db_path)
        for topic in topics:
            # Assuming partition 0 for all topics as per Kafka setup
            partition = 0
            result = con.execute(
                """
                SELECT last_offset
                FROM kafka_offsets
                WHERE consumer_group_id = ? AND topic = ? AND partition = ?;
            """,
                [CONSUMER_GROUP_ID, topic, partition],
            ).fetchone()

            if result:
                last_offset = result[0]
                # Kafka consumer starts reading *after* the specified offset, so we give it the last processed one.
                # If last_offset is -1, it means start from the beginning (handled by Kafka client default).
                offsets[TopicPartition(topic, partition)] = last_offset
                logger.info(
                    "Found last processed offset for topic '%s' partition %d: %d",
                    topic,
                    partition,
                    last_offset,
                )
            else:
                # No offset found, will start from the earliest available offset by default
                offsets[TopicPartition(topic, partition)] = (
                    -1
                )  # -1 indicates start from the beginning for this library
                logger.info(
                    "No previous offset found for topic '%s' partition %d. Will start from earliest.",
                    topic,
                    partition,
                )

        con.close()
    except Exception as e:
        logger.error("❌ Failed to get last processed offsets from DuckDB: %s", e)
        # Decide whether to fatal or continue assuming no offsets exist
        # For robustness, let's fatal if we can't read offsets initially
        sys.exit(1)
    return offsets


def update_processed_offset(db_path: str, topic: str, partition: int, offset: int):
    """Updates the last processed offset for a topic/partition in DuckDB."""
    try:
        # Check if db_path is valid before connecting - Redundant now with top-level check, but safe.
        # if not isinstance(db_path, (str, os.PathLike)):
        #      raise TypeError(f"Invalid database path type: {type(db_path)}. Expected str or PathLike.")

        con = duckdb.connect(database=db_path)
        con.execute(
            """
            INSERT INTO kafka_offsets (consumer_group_id, topic, partition, last_offset, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (consumer_group_id, topic, partition)
            DO UPDATE SET last_offset = excluded.last_offset, updated_at = excluded.updated_at;
        """,
            [CONSUMER_GROUP_ID, topic, partition, offset],
        )
        con.commit()
        con.close()
        # logger.info("Updated offset for topic '%s' partition %d to %d", topic, partition, offset) # Too verbose
    except Exception as e:
        logger.error(
            "❌ Failed to update offset for topic '%s' partition %d to %d in DuckDB: %s",
            topic,
            partition,
            offset,
            e,
        )
        # This is a critical error. If this fails after writing to Iceberg,
        # the next run might re-process messages from the last *successfully committed* offset.
        # Iceberg's idempotency (appending same data multiple times)
        # and dbt's handling of duplicates in transformations can help mitigate this.


# --- Iceberg Writing Function ---


def write_batch_to_iceberg(catalog: Catalog, batch: list[dict]):
    """Writes a batch of processed messages to the appropriate Iceberg tables."""
    if not batch:
        return

    # Group messages by topic
    messages_by_topic = {}
    for msg_data in batch:
        topic = msg_data["topic"]
        if topic not in messages_by_topic:
            messages_by_topic[topic] = []
        messages_by_topic[topic].append(msg_data)

    # Process each topic's messages
    for topic, messages in messages_by_topic.items():
        table_name = None
        if topic == ELECTRICITY_TOPIC:
            table_name = ELECTRICITY_ICEBERG_TABLE
        elif topic == GAS_TOPIC:
            table_name = GAS_ICEBERG_TABLE
        else:
            logger.warning("Received batch for unexpected topic '%s'. Skipping.", topic)
            continue

        try:
            # Get the Iceberg table
            # Table identifier is namespace.table_name (e.g., default.raw_p1_electricity_iceberg)
            table = catalog.load_table(
                f"default.{table_name}"
            )  # Assuming 'default' namespace

            # Prepare data for PyArrow
            # We need to define the schema matching the Iceberg table
            # Note: PyArrow requires consistent types within a batch
            # Handle potential missing fields due to partial updates by ensuring all fields
            # are present with None/default values if not in the message.
            # This is a simplification; a more robust approach might involve schema evolution
            # or handling partial records explicitly in transformations.

            # Infer schema from the first message, or use a predefined schema
            # For simplicity, let's define the schema explicitly based on our Iceberg table DDL
            schema = pa.schema(
                [
                    ("kafka_topic", pa.string()),
                    ("kafka_partition", pa.int32()),
                    ("kafka_offset", pa.int64()),
                    (
                        "kafka_timestamp",
                        pa.timestamp("us", tz="UTC"),
                    ),  # Kafka timestamp is milliseconds, convert to microseconds
                    (
                        "processing_timestamp",
                        pa.timestamp("us", tz="UTC"),
                    ),  # Processing timestamp from JSON
                    ("telegram_timestamp", pa.string()),
                    ("raw_data", pa.string()),
                ]
            )

            # Prepare columns for PyArrow Table
            # Initialize lists for each column
            col_data = {field.name: [] for field in schema}

            for msg_data in messages:
                raw_json_str = msg_data["value"].decode("utf-8")
                parsed_json = json.loads(raw_json_str)

                # Populate columns, handling potential missing keys with None
                col_data["kafka_topic"].append(msg_data["topic"])
                col_data["kafka_partition"].append(msg_data["partition"])
                col_data["kafka_offset"].append(msg_data["offset"])
                # Convert Kafka timestamp (ms) to microseconds for PyArrow timestamp
                col_data["kafka_timestamp"].append(
                    datetime.fromtimestamp(
                        msg_data["timestamp_ms"] / 1000, tz=timezone.utc
                    )
                )

                # Parse processing_timestamp from JSON string
                processing_ts_str = parsed_json.get("processingTimestampUTC")
                if processing_ts_str:
                    try:
                        # Assuming ISO format with nanoseconds
                        col_data["processing_timestamp"].append(
                            datetime.fromisoformat(
                                processing_ts_str.replace("Z", "+00:00")
                            )
                        )
                    except ValueError:
                        logger.warning(
                            "Could not parse processingTimestampUTC '%s'. Setting to None.",
                            processing_ts_str,
                        )
                        col_data["processing_timestamp"].append(None)
                else:
                    col_data["processing_timestamp"].append(None)

                col_data["telegram_timestamp"].append(
                    parsed_json.get("telegramTimestamp")
                )  # Can be None
                col_data["raw_data"].append(raw_json_str)

            # Create PyArrow Table
            arrow_table = pa.Table.from_pydict(col_data, schema=schema)

            # Use PyIceberg to write the Arrow Table to the Iceberg table
            # PyIceberg handles creating Parquet files and updating metadata
            logger.info(
                "Writing batch of %d messages to Iceberg table '%s'...",
                len(messages),
                table_name,
            )
            table.append(arrow_table)

            logger.info("✅ Successfully wrote batch to Iceberg table '%s'", table_name)

            # After successful write, update offsets in DuckDB
            # Find the highest offset in this batch for this topic/partition
            highest_offset = max(msg_data["offset"] for msg_data in messages)
            # Assuming single partition 0 for simplicity
            update_processed_offset(DUCKDB_OFFSET_PATH, topic, 0, highest_offset)

        except Exception as e:
            logger.error(
                "❌ Failed to write batch to Iceberg table '%s': %s", table_name, e
            )
            # If writing to Iceberg fails, we do NOT update the offset in DuckDB.
            # This batch (and potentially subsequent ones) will be re-read on the next run.
            # Iceberg's append operation is designed to be idempotent at the file level,
            # but re-processing messages might lead to duplicate rows in the raw table.
            # This is acceptable in the raw layer, as duplicates can be handled by dbt transformations.


# --- Main Consumption Logic ---


def run_consumer():
    """Main function to run the Kafka consumer and write to Iceberg."""
    logger.info("Starting Kafka consumer...")

    # Initialize DuckDB offsets database
    # This call now happens AFTER the configuration check
    initialize_duckdb_offsets(DUCKDB_OFFSET_PATH)

    # Get last processed offsets from DuckDB
    topics_to_subscribe = [ELECTRICITY_TOPIC, GAS_TOPIC]
    start_offsets = get_last_processed_offsets(DUCKDB_OFFSET_PATH, topics_to_subscribe)

    # Configure Kafka Consumer
    # auto_offset_reset='earliest' is a fallback if no committed offset is found (or if using -1 from DB)
    # enable_auto_commit=False because we manage offset commits manually in DuckDB
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER_ADDRESS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",  # Start from earliest if no committed offset in Kafka
        enable_auto_commit=False,  # Disable Kafka's auto-commit
        value_deserializer=lambda x: x,  # Receive raw bytes, will deserialize JSON later
        # Configure consumer to start from specific offsets
        # This must be done *after* the consumer is created and has joined the group/assigned partitions.
        # A common pattern is to use assign() and seek().
        # However, the python-kafka library's high-level consumer with group_id
        # handles seeking automatically if you provide initial offsets during assignment.
        # Let's use assign and seek explicitly for clarity.
        # We subscribe to topics first, then get assignments, then seek.
    )

    # Subscribe to the topics
    consumer.subscribe(topics=topics_to_subscribe)

    # Wait until partitions are assigned before seeking
    logger.info("Waiting for partition assignments...")
    while not consumer.assignment():
        if shutdown_flag.is_set():
            logger.info("Shutdown requested while waiting for assignments.")
            consumer.close()
            return
        # Poll briefly to trigger assignment
        consumer.poll(timeout_ms=100)

    logger.info("Partitions assigned: %s", consumer.assignment())

    # Seek to the last processed offsets
    for tp, offset in start_offsets.items():
        # Seek to offset + 1 because the stored offset is the *last processed* message.
        # We want to start reading from the *next* message.
        # If offset is -1 (kafka.FirstOffset equivalent in our scheme),
        # the default 'earliest' auto_offset_reset will handle starting from the beginning.
        if offset != -1:
            # Use seek(partition, offset) - offset is the *next* message offset
            consumer.seek(tp, offset + 1)
            logger.info(
                "Seeked topic '%s' partition %d to offset %d",
                tp.topic,
                tp.partition,
                offset + 1,
            )
        else:
            logger.info(
                "Starting from earliest available offset for topic '%s' partition %d.",
                tp.topic,
                tp.partition,
            )

    # Load Iceberg Catalog
    try:
        # Corrected: Load the catalog by name defined in ~/.pyiceberg.yaml
        logger.info("Loading Iceberg catalog 'local' from config...")
        catalog = load_catalog(name="local")  # Use the name from the YAML file
        logger.info("✅ Successfully loaded Iceberg catalog.")
    except Exception as e:
        logger.error("❌ Failed to load Iceberg catalog: %s", e)
        consumer.close()
        sys.exit(1)

    # Message consumption loop
    current_batch = []
    last_batch_time = time.time()

    logger.info("Starting message polling loop...")

    while not shutdown_flag.is_set():
        # Poll for messages with a timeout
        # The timeout here is important so the loop doesn't block indefinitely
        # and can check the shutdown_flag periodically.
        messages = consumer.poll(
            timeout_ms=1000, max_records=BATCH_SIZE
        )  # Poll for up to 1 second or BATCH_SIZE records

        if messages:
            # Process received messages
            # messages is a dict like {TopicPartition(...): [Message(...), ...]}
            for tp, msgs in messages.items():
                for msg in msgs:
                    # Append message data needed for processing and writing
                    current_batch.append(
                        {
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "value": msg.value,  # Raw bytes
                            "timestamp_ms": msg.timestamp,  # Kafka timestamp in milliseconds
                            # Note: We don't need to deserialize the JSON here yet,
                            # we'll do it in write_batch_to_iceberg to keep batch processing together.
                        }
                    )
                    # logger.debug("Added message to batch: %s-%d@%d", msg.topic, msg.partition, msg.offset) # Too verbose

        # Check if batch size is reached or batch timeout occurred
        if len(current_batch) >= BATCH_SIZE or (
            time.time() - last_batch_time >= BATCH_TIMEOUT_SECONDS
            and len(current_batch) > 0
        ):
            logger.info(
                "Processing batch (size=%d, time_since_last=%.2f)...",
                len(current_batch),
                time.time() - last_batch_time,
            )
            write_batch_to_iceberg(catalog, current_batch)
            current_batch = []  # Clear the batch
            last_batch_time = time.time()  # Reset the timer

        # If no messages were received in the poll, the timer still needs to be checked
        if (
            not messages
            and len(current_batch) > 0
            and (time.time() - last_batch_time >= BATCH_TIMEOUT_SECONDS)
        ):
            logger.info(
                "Batch timeout (no new messages): Processing batch of %d messages...",
                len(current_batch),
            )
            write_batch_to_iceberg(catalog, current_batch)
            current_batch = []  # Clear the batch
            last_batch_time = time.time()  # Reset the timer

    # --- Shutdown ---
    logger.info("Shutdown flag set. Processing final batch...")
    # Process any remaining messages in the batch before exiting
    if len(current_batch) > 0:
        logger.info(
            "Processing final batch of %d messages before exiting...",
            len(current_batch),
        )
        write_batch_to_iceberg(catalog, current_batch)
        logger.info("Final batch processed.")

    # Close the Kafka consumer
    logger.info("Closing Kafka consumer...")
    consumer.close()
    logger.info("Kafka consumer closed.")

    logger.info("Consumer application finished.")


if __name__ == "__main__":
    run_consumer()
