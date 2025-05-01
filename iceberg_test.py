import os

os.environ["PYICEBERG_HOME"] = os.getcwd()

from pyiceberg.catalog import load_catalog

catalog = load_catalog(name="local")

print(catalog.properties)
