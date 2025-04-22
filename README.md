# odibi_de_v2

[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Build](https://img.shields.io/badge/build-passing-brightgreen)]()

A modular, production-grade data engineering framework for Spark and Pandas.
Designed with extensibility, logging, cloud-native flexibility, and maintainability in mind.

---

# odibi_de_v2: Modular Python Data Engineering Framework

Welcome to the GitHub repository for `odibi_de_v2`, a modular Python framework designed to streamline data engineering tasks. This framework provides a comprehensive suite of tools and utilities to facilitate data ingestion, processing, and storage across various platforms and technologies including Spark, Pandas, REST APIs, SQL databases, and Azure Data Lake Storage (ADLS).

## Badges
![GitHub stars](https://img.shields.io/github/stars/henryodibi11/odibi_de_v2?style=social)
![GitHub forks](https://img.shields.io/github/forks/henryodibi11/odibi_de_v2?style=social)
![GitHub issues](https://img.shields.io/github/issues/henryodibi11/odibi_de_v2)
![Python version](https://img.shields.io/badge/python-3.8+-blue.svg)
![Build status](https://img.shields.io/badge/build-passing-brightgreen.svg)

## Table of Contents
- [Modules](#modules)
  - [Utils](#utils)
  - [Connector](#connector)
  - [Pandas Utils](#pandas_utils)
  - [Spark Utils](#spark_utils)
  - [Core](#core)
  - [Logger](#logger)
  - [Ingestion](#ingestion)
  - [Databricks](#databricks)
  - [Storage](#storage)
- [Design Highlights](#design-highlights)

## Modules

### Module Table
| Module        | Description |
|---------------|-------------|
| `utils`       | Utility functions for common tasks in data engineering and software development. |
| `connector`   | Classes for connections to various data storage systems. |
| `pandas_utils`| Utility functions for data manipulation within Pandas. |
| `spark_utils` | Utility functions for handling Spark DataFrames. |
| `core`        | Core framework functionalities for data operations. |
| `logger`      | Advanced logging and metadata management. |
| `ingestion`   | Classes for data ingestion and reading. |
| `databricks`  | Tools for data processing within Spark-based frameworks. |
| `storage`     | Classes for saving data across different storage frameworks. |

### Detailed Module Descriptions

#### `utils`
This module contains a collection of utility functions designed to facilitate various common tasks in data engineering and software development...

#### `connector`
This module contains a collection of Python classes designed to facilitate connections to various data storage systems...

#### `pandas_utils`
This module contains a collection of utility functions designed to facilitate common data manipulation and validation tasks within a Pandas DataFrame context...

#### `spark_utils`
This module contains a collection of utility functions designed to manipulate and analyze Spark DataFrames within a data engineering context...

#### `core`
This module contains a modular Python data engineering framework designed to facilitate data reading, writing, and transformation across various file formats and cloud services...

#### `logger`
This module contains a Python module designed for advanced logging and metadata management within a data engineering framework...

#### `ingestion`
This module contains a comprehensive suite of Python classes designed for data ingestion and reading within a modular data engineering framework...

#### `databricks`
This module contains a Python module designed for data ingestion and processing within a Spark-based data engineering framework...

#### `storage`
This module contains a suite of classes designed for saving data across different storage frameworks and formats within a Python-based data engineering framework...

## Design Highlights
`odibi_de_v2` incorporates several key architectural patterns to enhance functionality and ease of use:

- **Decorators for Validation/Logging**: Used extensively across modules to ensure data integrity and to provide detailed logging.
- **Unified Connector System**: A robust system that simplifies connections to various data storage systems, ensuring flexibility and scalability.
- **Config-driven Ingestion**: Framework configurations drive the ingestion process, allowing for dynamic adjustments to data sources and formats.
- **Support for Multiple Technologies**: Seamless integration with Spark, Pandas, REST APIs, SQL databases, and ADLS to cater to diverse data engineering needs.

This framework is designed to be highly modular, allowing users to plug in different components as needed to suit their specific data engineering challenges.
