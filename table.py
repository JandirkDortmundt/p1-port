import pyiceberg

# Try to create a table directly
try:
    from pyiceberg.table import Table

    print("Table class is available")
except ImportError:
    print("Table class is not available")

try:
    # See if there's any catalog-related functionality
    for attr in dir(pyiceberg):
        if "catalog" in attr.lower():
            print(f"Found catalog-related attribute: {attr}")
except Exception as e:
    print(f"Error searching for catalog: {e}")
