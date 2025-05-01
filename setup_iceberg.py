import os
import logging
import sys

# Third-party library
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    TimestamptzType,
)

# Import PartitionSpec and PartitionField if you plan to use partitioning
# from pyiceberg.partitioning import PartitionSpec, PartitionField

# Note: PYICEBERG_HOME is typically set as an environment variable or implicitly
# when running from the directory containing .pyiceberg.yaml.
# Explicitly setting it here might be needed depending on your execution context.
# os.environ["PYICEBERG_HOME"] = os.getcwd() # Uncomment if needed


# --- Configuration ---
# Define the root directory for your Iceberg lakehouse warehouse.
# This should match the 'warehouse' path in your ~/.pyiceberg.yaml file
# (e.g., file://energy_lakehouse) and the path you plan to use in your
# consumer's .env file (ICEBERG_LAKEHOUSE_PATH).
# We use os.path.expanduser here for logging and ensuring the directory exists,
# but pyiceberg loads the actual path from the config file based on the catalog name.
ICEBERG_LAKEHOUSE_PATH = os.path.expanduser(
    "~/energy_lakehouse"
)  # Use your actual desired path


# Iceberg table names
ELECTRICITY_ICEBERG_TABLE = "raw_p1_electricity_iceberg"
GAS_ICEBERG_TABLE = "raw_p1_gas_iceberg"  # Correct variable name

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("iceberg_setup")

# --- Main Setup Logic ---


def setup_iceberg_lakehouse():
    """Sets up the Iceberg catalog (based on config) and creates the raw tables."""
    # Note: The actual lakehouse warehouse path is loaded from ~/.pyiceberg.yaml by pyiceberg,
    # but we use this variable for logging and ensuring the directory exists.
    logger.info("Setting up Iceberg lakehouse based on config file...")

    # Ensure the lakehouse warehouse root directory exists (based on our expectation from config)
    # This is a safety check. PyIceberg might also create it, but explicit is better.
    try:
        os.makedirs(ICEBERG_LAKEHOUSE_PATH, exist_ok=True)
        logger.info(
            "✅ Ensured lakehouse warehouse directory exists at %s",
            ICEBERG_LAKEHOUSE_PATH,
        )
    except Exception as e:
        logger.error("❌ Failed to ensure lakehouse warehouse directory exists: %s", e)
        sys.exit(1)

    # Define the schema for the raw tables.
    # This schema must match the one you intend to use when writing data.
    raw_data_schema = Schema(
        NestedField(1, "kafka_topic", StringType(), required=False),
        NestedField(2, "kafka_partition", IntegerType(), required=False),
        NestedField(3, "kafka_offset", LongType(), required=False),
        NestedField(
            4, "kafka_timestamp", TimestamptzType(), required=False
        ),  # Kafka timestamp (ms)
        NestedField(
            5, "processing_timestamp", TimestamptzType(), required=False
        ),  # Timestamp from Go/Python app
        NestedField(
            6, "telegram_timestamp", StringType(), required=False
        ),  # Timestamp from P1 telegram
        NestedField(7, "raw_data", StringType(), required=False),  # Raw JSON string
    )
    logger.info("Defined raw data schema:\n%s", raw_data_schema)

    # Load the catalog by name from the config file (~/.pyiceberg.yaml)
    try:
        # Corrected: Load the catalog by the name defined in ~/.pyiceberg.yaml
        logger.info("Loading Iceberg catalog 'local' from config...")
        catalog = load_catalog(name="local")  # Use the name from the YAML file
        logger.info("✅ Successfully loaded Iceberg catalog.")
    except Exception as e:
        logger.error("❌ Failed to load Iceberg catalog: %s", e)
        # This could happen if the config file is missing, the catalog name is wrong,
        # or the config itself is invalid (e.g., warehouse path incorrect).
        sys.exit(1)

    # --- Create Namespace ---
    # Create the 'default' namespace if it doesn't exist.
    # Tables are created within namespaces.
    namespace_name = "default"
    try:
        logger.info("Ensuring namespace '%s' exists...", namespace_name)
        catalog.create_namespace_if_not_exists(namespace_name)
        logger.info("✅ Ensured namespace '%s' exists.", namespace_name)
    except Exception as e:
        logger.error("❌ Failed to create namespace '%s': %s", namespace_name, e)
        sys.exit(1)
    # --- End Create Namespace ---

    # Create the raw electricity table within the namespace (if it doesn't exist)
    try:
        logger.info(
            "Creating Iceberg table '%s.%s' (if not exists)...",
            namespace_name,
            ELECTRICITY_ICEBERG_TABLE,
        )
        # Corrected: Use create_table_if_not_exists for idempotency
        catalog.drop_table(f"{namespace_name}.{ELECTRICITY_ICEBERG_TABLE}")
        electricity_table = catalog.create_table_if_not_exists(
            f"{namespace_name}.{ELECTRICITY_ICEBERG_TABLE}",  # Table identifier (namespace.table_name)
            raw_data_schema,
            # Define partitioning if needed (e.g., by date or hour from processing_timestamp)
            # partitioning_spec=PartitionSpec(PartitionField(4, 'processing_timestamp', 'day')), # Example: partition by day
            # Set table properties if needed
            # properties={"write.parquet.compression-codec": "zstd"}
        )
        logger.info(
            "✅ Ensured Iceberg table '%s.%s' exists.",
            namespace_name,
            ELECTRICITY_ICEBERG_TABLE,
        )
    except Exception as e:
        logger.error(
            "❌ Failed to create or load Iceberg table '%s.%s': %s",
            namespace_name,
            ELECTRICITY_ICEBERG_TABLE,
            e,
        )
        # Decide if you want to exit or continue if one table creation fails
        sys.exit(1)

    # Create the raw gas table within the namespace (if it doesn't exist)
    try:
        logger.info(
            "Creating Iceberg table '%s.%s' (if not exists)...",
            namespace_name,
            GAS_ICEBERG_TABLE,
        )  # Corrected variable name
        # Corrected: Use create_table_if_not_exists for idempotency
        catalog.drop_table(f"{namespace_name}.{GAS_ICEBERG_TABLE}")
        gas_table = catalog.create_table_if_not_exists(
            f"{namespace_name}.{GAS_ICEBERG_TABLE}",  # Table identifier (namespace.table_name) # Corrected variable name
            raw_data_schema,  # Use the same schema
            # Define partitioning if needed (e.g., by date or hour from processing_timestamp)
            # partitioning_spec=PartitionSpec(PartitionField(4, 'processing_timestamp', 'day')), # Example: partition by day
            # properties={"write.parquet.compression-codec": "zstd"}
        )
        logger.info(
            "✅ Ensured Iceberg table '%s.%s' exists.",
            namespace_name,
            GAS_ICEBERG_TABLE,
        )
    except Exception as e:
        logger.error(
            "❌ Failed to create or load Iceberg table '%s.%s': %s",
            namespace_name,
            GAS_ICEBERG_TABLE,
            e,
        )
        sys.exit(1)

    logger.info("Iceberg lakehouse setup complete.")


if __name__ == "__main__":
    # Note: This script assumes you have already created the DuckDB offsets database
    # and the kafka_offsets table using the DuckDB CLI as outlined previously.
    # This script *only* sets up the Iceberg part.

    setup_iceberg_lakehouse()  # No need to pass path, it's in the config
