# Sales Data Analysis of Apple Products

## Overview

This repository contains Data Engineering solution using ETL (Extract, Transform, Load) implementation for the sales data analysis of Apple products. The solution is designed to handle diverse data formats and is implemented on Databricks using PySpark, Python, and Databricks utilities.Factory Method Design Pattern has been implemented for reading and writing data based on data format. It provides a standardized interface to handle different data formats seamlessly.

As a part of this project we are solving 4 business problem statements:
- Customers who have bought AirPods after buying iPhone.
- Customers who have bought both AirPods and iPhone.
- List all the products bought by customers after the initial purchase.
- Determine the Average time delay buying an iPhone and buying AirPods for all customers.

## Data Model

- **Dimensional Data**: Customer data is stored as a Delta table on Databricks.
- **Fact Data**: Transaction data is available in CSV format.

## ETL Solution Components

### 1. Delta Table Creation Notebook
This notebook provides the steps to create a Delta table for customer data on Databricks. It includes schema definition, table creation, and data ingestion into the Delta table.

### 2. Reader Factory Notebook
This notebook contains the Factory Method Design Pattern implementation for reading data from various sources. It provides a standardized interface to handle different data formats seamlessly. To read more about Factory Method Design, please visit https://www.geeksforgeeks.org/factory-method-for-designing-pattern/

### 3. Extractor Notebook
The Extractor notebook includes the implementation code for extracting data from each source. It utilizes the Reader Factory to read data in the appropriate format.

### 4. Loader Factory Notebook
Similar to the Reader Factory, the Loader Factory notebook defines a set of methods for loading data into the target system. It ensures that the data is loaded correctly regardless of the destination format.

### 5. Loader Notebook
The Loader notebook contains the actual loading logic for each data destination. It uses the Loader Factory to determine the correct loading procedure.

### 6. Transformer Notebook
This notebook is responsible for implementing various data transformations based on the specific requirements or business cases. It is the core component where data is shaped and prepared for analysis.

### 7. ETL Workflow Notebook
The ETL Workflow notebook outlines the steps to design and execute the ETL workflows for different requirements. It acts as a blueprint for the ETL process.

### 8. Workflow Runner Notebook
The Workflow Runner notebook orchestrates the execution of the ETL workflows using Databricks utilities. It features widgets that allow users to select and trigger different workflows based on their names.

## Usage

To use this ETL solution, follow these steps:

1. **Set Up Environment**: Ensure that Databricks and all necessary libraries are set up and configured correctly.
2. **Create Delta Table**: Follow the steps in the Delta Table Creation Notebook to set up the customer data table.
3. **Data Ingestion**: Use the Reader Factory and Extractor notebooks to ingest data from the CSV files and other sources.
4. **Data Transformation**: Apply the required transformations using the Transformer notebook to prepare the data for analysis.
5. **Data Loading**: Load the transformed data into the target system using the Loader Factory and Loader notebooks.
6. **Workflow Execution**: Use the Workflow Runner notebook to orchestrate and execute the ETL workflows. Select the desired workflow using the provided widgets.

## Prerequisites

- Databricks environment
- PySpark
- Python
- Familiarity with the Factory Method Design Pattern
- Understanding of Delta tables and CSV file handling

## Contributions

Contributions to this project are welcome. Please ensure that any contributions follow the existing design patterns and coding standards.

## Contact

For any queries or support related to this Data Engineering solution, please contact raysachin1997@gmail.com
