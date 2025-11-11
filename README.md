# datacontracts

- [datacontracts](#datacontracts)
  - [Usefull pipenv commands](#usefull-pipenv-commands)
  - [Usefull uv and ruff commands](#usefull-uv-and-ruff-commands)
  - [Project structure](#project-structure)

A library for implementing and validating data contract content.

The libray contains methods to parse/load contracts in yaml format and validate the structure of the contract.

The library also contains functionality to emit Spark StructTypes, Databricks SQL for Creating and altering tables from specifications in the contract and Markdown documentation of the contract content.

Validation of data in Spark data frames is still a work in progress, and will be added at a later stage.

## Usefull pipenv commands

``` bash
# Initial setup - this reads from pyproject.toml
pipenv install -e ".[dev]"

# Running examples
# Uncomment the function you would like to test in demo_load_and_emit.py
pipenv run python examples/demo_load_and_emit.py

# Check formatting without changes
pipenv run black --check src/

# Format code
pipenv run black src/

# Sort imports
pipenv run isort src/

# Type check (Personally I would like to do this more, but...)
pipenv run mypy src/
```

## Usefull uv and ruff commands

``` bash
.venv\Scripts\activate
```

``` bash
# Sort imports
ruff check --select I --fix

# Running the demos in the tools folder (module)
uv run examples/demo_load_and_emit.py
```

## Project structure

``` bash
.
├── docs
├── examples  # FOR DEMO PURPOSES: Runnable examples
│   ├── contracts  # Contract implementations are placed here (can live elsewhere)
│   │   ├── json_schemas  # Schemas for validating yaml and providing intellisense
│   │   │   └── data-table-contract.schema.json
│   │   └── yaml  # Contains the contracts in yaml format
│   │       ├── service_hr_address.yaml  # Contract example
│   │       └── service__hr__employee.yaml  # Contract example
│   └── demo_load_and_emit.py  # Examples of library functionality
├── src
│   └── datacontracts
│       ├── emitters
│       │   ├── templates  # Jinja2 templates to emit Data Contract artifacts
│       │   │   ├── databricks
│       │   │   │   ├── alter_table.sql.j2
│       │   │   │   └── create_table.sql.j2
│       │   │   └── docs  # Documentation templates for Data Contracts
│       │   │       └── datatable_contract_docs.md.j2
│       │   ├── __init__.py
│       │   ├── databricks_sql_emitter.py  # Methods to emit Databricks SQL (Create/Modify) 
│       │   ├── json_emitter.py  # Methods to emit Data Contract as json and json Schema
│       │   ├── markdown_docs_emitter.py  # Methods to emit Data Contracts as markdown
│       │   ├── spark_structtype_emitter.py  # Methods to emit Data Contracts as Spark 
│       │   └── yaml_emitter.py  # Methods to emit Data Contracts as yaml
│       ├── loaders   # Methods to load and instantiate DataContract objects
│       │   ├── __init__.py
│       │   ├── structtype_loader.py  # Methods to instantiate from Spark StructType
│       │   └── yaml_loader.py  # Methods to instantiate from yaml
│       ├── models  # Pydantic models and model components e.g. enums
│       │   ├── __init__.py
│       │   ├── base.py  # Pydantic Contract components
│       │   ├── datatable.py  # Pydantic DataTableContract models
│       │   └── option_types.py  # Enums
│       ├── __init__.py
│       ├── type_converter.py  # Type conversions
│       └── validation.py  # Validation rule implementations (data)
├── Pipfile
├── Pipfile.lock
├── pyproject.toml
└── README.md  # You're already here - thank you for reading :-)
```
