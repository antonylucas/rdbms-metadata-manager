import csv
from sqlalchemy import create_engine, MetaData, inspect
from dotenv import load_dotenv
import os
from urllib.parse import urlparse

# Loading variables
load_dotenv()

db_url = os.getenv("DB_URL")
if not db_url:
    raise ValueError("Connect String (DB_URL) not set. Please provide it in the .env file.")

def get_database_name(db_url):
    """Extracts the database (schema) name from the SQLAlchemy connection string."""
    parsed_url = urlparse(db_url)
    database_name = parsed_url.path.lstrip('/')
    if database_name:
        return database_name
    raise ValueError("Could not extract database name from DB_URL.")

database_name = get_database_name(db_url)

# Function to detect schema flaws
def detect_schema_flaws(engine):
    issues = []
    metadata = MetaData()
    metadata.reflect(bind=engine)
    inspector = inspect(engine)

    for table_name in metadata.tables.keys():
        indexes = inspector.get_indexes(table_name)
        foreign_keys = inspector.get_foreign_keys(table_name)
        indexed_columns = {col for index in indexes for col in index.get("column_names", [])}
        foreign_key_columns = {fk['constrained_columns'][0] for fk in foreign_keys if fk['constrained_columns']}
        table = metadata.tables[table_name]

        for column in table.columns:
            column_type = column.type.__class__.__name__

            # Rule 1: Detect large VARCHAR/TEXT columns without an index
            if column_type in ["VARCHAR", "TEXT"] and \
               hasattr(column.type, "length") and column.type.length and column.type.length >= 255 and \
               column.name not in indexed_columns and not column.unique:
                issues.append({
                    "table": table_name,
                    "column": column.name,
                    "issue type": "Query performance - missing index",
                    "issue": f"Large {column_type} column '{column.name}' in '{table_name}' is not indexed.",
                    "recommendation": f"Add an index on '{table_name}({column.name})' to improve query performance."
                })

            # Rule 2: Check for columns ending or starting with 'id' that are not keys or indexes
            if (column.name.lower().endswith("id") or column.name.lower().startswith("id")) and \
               column.name not in foreign_key_columns and column.name not in indexed_columns and not column.primary_key:
                issues.append({
                    "table": table_name,
                    "column": column.name,
                    "issue type": "Normalization - Data integrity",
                    "issue": f"Potential foreign key column '{column.name}' is not properly defined.",
                    "recommendation": f"Define a foreign key constraint and index for '{column.name}' referencing "
                                      f"the appropriate table and add the correct kind of index. "
                })

            # Rule 3: Check for potential monetary columns without DECIMAL type
            if any(keyword in column.name.lower() for keyword in [
                'price', 'amount', 'total', 'cost', 'value', 'balance', 'rate']):
                if column_type not in ['DECIMAL', 'NUMERIC']:
                    issues.append({
                        "table": table_name,
                        "column": column.name,
                        "issue type": "Data type - Precision error",
                        "issue": f"Monetary column '{column.name}' is of type '{column_type}', expected DECIMAL or NUMERIC.",
                        "recommendation": f"Consider changing the column '{table_name}({column.name})' to DECIMAL"
                                          f" or NUMERIC for better precision in monetary calculations."
                    })

            # Rule 4: Check for columns in the metadata dictionary with correct data types expected
            expected_types = {
                "rating": "FLOAT",
                "created_at": "DATETIME",
                "order_date": "DATETIME"
            }
            if column.name.lower() in expected_types and column_type != expected_types[column.name.lower()]:
                issues.append({
                    "table": table_name,
                    "column": column.name,
                    "issue type": "Data type mismatch",
                    "issue": f"Column '{column.name}' has type '{column_type}'"
                             f", expected '{expected_types[column.name.lower()]}'.",
                    "recommendation": f"Change column '{table_name}({column.name})' "
                                      f"to '{expected_types[column.name.lower()]}' to match the expected type defined"
                })

            # Rule 5: Check for columns that should not allow NULL values
            non_nullable_columns = ["email", "price", "total_amount", "order_date", "rating"]
            if column.name.lower() in non_nullable_columns and column.nullable:
                issues.append({
                    "table": table_name,
                    "column": column.name,
                    "issue type": "Data Integrity - NULL values not allowed",
                    "issue": f"Critical column '{column.name}' allows NULL values.",
                    "recommendation": f"Alter column '{table_name}({column.name})' to NOT NULL to maintain data "
                                      f"integrity."
                })

    return issues

# Method to structure and export issues report to CSV
def export_to_csv(issues, filename):
    header = ["Table", "Column", "Issue Type", "Issue", "Recommendation"]
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        for issue in issues:
            writer.writerow([
                issue["table"], issue["column"], issue["issue type"], issue["issue"], issue["recommendation"]
            ])
    print(f"Results exported to {filename}")


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
            if os.getenv("EXPORT_TO_CSV", "NO").strip().upper() == "YES":
                export_to_csv(issues, f"exports/{database_name}_schema_issues.csv")
        else:
            print(f"No issues detected in schema \"{database_name}\".")
