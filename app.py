import csv
from sqlalchemy import create_engine, MetaData, inspect
from dotenv import load_dotenv
import os
from urllib.parse import urlparse


load_dotenv()


db_url = os.getenv("DB_URL")
if not db_url:
    raise ValueError("Connect String (DB_URL) not set. Please provide it in the .env file.")


def get_database_name(db_url):
    """
    Extracts the database (schema) name from the SQLAlchemy connection string.
    """
    parsed_url = urlparse(db_url)

    database_name = parsed_url.path.lstrip('/')
    if database_name:
        return database_name
    raise ValueError("Could not extract database name from DB_URL.")

database_name = get_database_name(db_url)

# Function to detect schema flaws
def detect_schema_flaws(engine):
    issues = []

    # Reflect the database metadata
    metadata = MetaData()
    metadata.reflect(bind=engine)

    inspector = inspect(engine)

    # Loop through all schema tables
    for table_name in metadata.tables.keys():
        # Get table metadata and indexes
        indexes = inspector.get_indexes(table_name)
        foreign_keys = inspector.get_foreign_keys(table_name)

        indexed_columns = {col for index in indexes for col in index.get("column_names", [])}
        foreign_key_columns = {fk['constrained_columns'][0] for fk in foreign_keys if fk['constrained_columns']}

        # Get table columns
        table = metadata.tables[table_name]

        # Rule 1: Detect large VARCHAR/TEXT columns without an index
        for column in table.columns:
            if column.type.__class__.__name__ in ["VARCHAR", "TEXT"] and \
               hasattr(column.type, "length") and column.type.length and column.type.length >= 255 and \
               column.name not in indexed_columns and not column.unique:
                issues.append({
                    "table": table_name,
                    "column": column.name,
                    "issue type": "Query performance - missing index",
                    "issue": f"The column '{column.name}' in table '{table_name}' is a large VARCHAR/TEXT but is not indexed.",
                    "recommendation": f"Consider adding an index on '{table_name}({column.name})' to improve query performance."
                })

        # Rule 2: Check for columns ending or starting with 'id' that are not keys or indexes
        for column in table.columns:
            column_name = column.name.lower()
            if (column_name.endswith('id') or column_name.startswith('id')) and \
                    column.name not in foreign_key_columns and \
                    column.name not in indexed_columns and \
                    not column.primary_key:
                issues.append({
                    "table": table_name,
                    "column": column.name,
                    "issue type": "Normalization - Data integrity",
                    "issue": f"The column '{column.name}' in table '{table_name}' might be a foreign key but is not defined as one.",
                    "recommendation": f"Define a foreign key constraint on '{table_name}({column.name})' referencing the appropriate table and add the correct kind of index."
                })

        # Rule 3: Check for potential monetary columns without DECIMAL type
        for column in table.columns:
            if any(keyword in column.name.lower() for keyword in [
                'price', 'amount', 'total', 'cost', 'value', 'charge', 'fee', 'revenue', 'income', 'expense',
                'budget', 'payment', 'tax', 'discount', 'rate', 'fund', 'balance', 'credit', 'debit', 'sale',
                'funds', 'profit', 'capital', 'commission', 'wage', 'salary']):
                if column.type.__class__.__name__ not in ['DECIMAL', 'NUMERIC']:
                    issues.append({
                        "table": table_name,
                        "column": column.name,
                        "issue type": "Data type - Precision error",
                        "issue": f"The column '{column.name}' in table '{table_name}' is storing monetary values but is not of type DECIMAL or NUMERIC.",
                        "recommendation": f"Consider changing the data type of '{table_name}({column.name})' to DECIMAL or NUMERIC for better precision in monetary calculations."
                    })

        # Rule 4: Check for columns in the metadata dictionary with correct data types expected
        for column in table.columns:
            # Dictionary to store columns and their expected types
            column_types = {
                "rating": "FLOAT",
                "created_at": "DATETIME",
                "order_date": "DATETIME"
                # Add more columns and their expected types if necessary
            }

            if column.name.lower() in column_types:
                expected_type = column_types[column.name.lower()]
                current_type = column.type.__class__.__name__

                if current_type != expected_type:
                    issues.append({
                        "table": table_name,
                        "column": column.name,
                        "issue type": "Data type mismatch",
                        "issue": f"The column '{column.name}' in table '{table_name}' is of type '{current_type}', but it should be of type '{expected_type}'.",
                        "recommendation": f"Change the data type of '{table_name}({column.name})' to '{expected_type}' to match the expected type."
                    })

        # Rule 5: Check for columns that should not allow NULL values
        non_nullable_columns = [
            "email", "price", "total_amount", "order_date", "rating",
            "username", "product_name" # Add more columns based on transactional rules and restrictions for the business
        ]

        for column in table.columns:
            if column.name.lower() in non_nullable_columns:
                if column.nullable:
                    issues.append({
                        "table": table_name,
                        "column": column.name,
                        "issue type": "Data Integrity - not allowed NULL values",
                        "issue": f"The column '{column.name}' in table '{table_name}' allows NULL values, but it should not.",
                        "recommendation": f"Alter the column '{table_name}({column.name})' to NOT NULL to maintain data integrity."
                    })

    return issues


# Function to structure and export issues report to CSV
def export_to_csv(issues, filename):
    header = ["Table", "Column", "Issue Type", "Issue", "Recommendation"]

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(header)

        for issue in issues:
            writer.writerow(
                [issue["table"], issue["column"], issue["issue type"], issue["issue"], issue["recommendation"]])

    print(f"Results have been exported to {filename}")


if __name__ == "__main__":
    engine = create_engine(db_url)

    with engine.connect() as connection:
        issues = detect_schema_flaws(engine)

        if issues:
            print(f"Schema \"{database_name}\" issues detected:")
            for issue in issues:
                print(f"Table: {issue['table']}")
                print(f"Column: {issue['column']}")
                print(f"Issue Type: {issue['issue type']}")
                print(f"Issue: {issue['issue']}")
                print(f"Recommendation: {issue['recommendation']}\n")

            export_to_csv_flag = os.getenv("EXPORT_TO_CSV", "NO").strip().upper()

            if export_to_csv_flag == "YES":
                export_to_csv(issues, filename=f"exports/{database_name}_schema_issues.csv")
            else:
                print("No export selected.")
        else:
            print(f"No issues detected in schema \"{database_name}\".")
