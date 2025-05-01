import pyiceberg
import sys
import importlib

print(f"PyIceberg version: {pyiceberg.__version__}")
print(f"PyIceberg path: {pyiceberg.__path__}")
print("\nAvailable modules in pyiceberg:")
for item in dir(pyiceberg):
    if not item.startswith("_"):
        print(f"- {item}")

# Try to find any catalog-related functionality
for module_name in [
    "pyiceberg.table",
    "pyiceberg.schema",
    "pyiceberg.catalog",
    "pyiceberg.io",
]:
    try:
        module = importlib.import_module(module_name)
        print(f"\nModule {module_name} is available")
        print(
            f"Content: {[item for item in dir(module) if not item.startswith('_')][:10]}..."
        )
    except ImportError:
        print(f"\nModule {module_name} is not available")

print("\nPython path:")
for path in sys.path:
    print(f"- {path}")
