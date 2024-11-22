# rdbms-metadata-manager  
Service for analyzing database schemas with the objective to automate the report of flaws and possible inconsistencies of differente kinds.

## Overview  

This document describes how to use the `app.py` script to identify and report potential issues in a database schema. This script is designed to automate the process of schema analysis and generate reports with recommendations related to:
* Normalization issues like no referential integrity between data objects
* Peformance issues related to querying data
* Column data type mismach and lack of precision

## Architecture Proposal

![Architecture proposal.png](Architecture%20proposal.png)

## Prerequisites  

- Create a `.env` file with the database connection URI in the project directory and configure for "YES" or "NO" if you want export the results to a CSV file. Example:  

    
    DB_URL=mysql+pymysql://user:password@host:port/database
    EXPORT_TO_CSV=YES

- Ensure the database is accessible on the network.
- Build the Docker image and start the container using the following command:
```bash    
      docker build -t rdbms-metadata-manager .
```

# Usage

- Run the script inside the container as follows:
      
      docker run --env-file .env --network <docker_network> -v $(pwd)/exports:/app/exports metadata-manager

**Obs.** In case of containers (database + rdbms-metadata-manager) in the same local environment you'll need to create a docker network and run both using the "--network" flag as the example.  

# Configuration Example

- Make sure to adjust the .env file to match your database connection details. Example:

      DB_URL=mysql+pymysql://root:system@challenge-mysql:3306/ecommerce_db
      EXPORT_TO_CSV=YES

# Output

The script will analyze the database schema, detect potential issues (potential normalization issues, missing indexes, nullable constraints, or improper data types), and generate a detailed report.
If configured, the report will be exported as a CSV file to the exports directory.

# Output Example

Detected issues are printed in the terminal, and a CSV file is saved in the exports directory if configured to do so. 

Example of issues printed to the terminal:
    
    Schema Issues Detected:

    Table: orders_test
    Column: user_id
    Issue Type: Normalization - Data integrity
    Issue: The column 'user_id' in table 'orders_teste' might be a foreign key but is not defined as one.
    Recommendation: Define a foreign key constraint on 'orders_teste(user_id)' referencing the appropriate table and add the correct kind of index.

    Table: Products
    Column: price
    Issue Type: Data type - Precision error
    Issue: The column 'price' in table 'Products' is storing monetary values but is not of type DECIMAL or NUMERIC.
    Recommendation: Consider changing the data type of 'Products(price)' to DECIMAL or NUMERIC for better precision in monetary calculations.

Example of csv file exported:

[schema_issues.csv](exports/schema_issues.csv)
