import psycopg
import csv


def load_csv_data(conn, table_name, csv_file, columns):
    with conn.cursor() as cur:
        with open(csv_file, "r") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            rows = list(reader)

            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING;
            """

            cur.executemany(insert_query, rows)
        print(f"Ingested {len(rows)} rows into {table_name}")


def main():
    # Connection details
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"

    # Connect to PostgreSQL
    conn = psycopg.connect(f"host={host} dbname={database} user={user} password={pas}")
    cur = conn.cursor()

    # SQL schema and indexes
    create_statements = [
        # accounts table
        """
        CREATE TABLE IF NOT EXISTS accounts (
            customer_id INT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            address_1 VARCHAR(100),
            address_2 VARCHAR(100),
            city VARCHAR(50),
            state VARCHAR(50),
            zip_code VARCHAR(10),
            join_date DATE
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_accounts_last_name ON accounts(last_name);",
        "CREATE INDEX IF NOT EXISTS idx_accounts_zip_code ON accounts(zip_code);",

        # products table
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY,
            product_code VARCHAR(10),
            product_description VARCHAR(100)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_products_product_code ON products(product_code);",

        # transactions table
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(50) PRIMARY KEY,
            transaction_date DATE,
            product_id INT,
            quantity INT,
            account_id INT,
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_transactions_product_id ON transactions(product_id);",
        "CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions(account_id);",
        "CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);"
    ]

    # Execute statements
    for stmt in create_statements:
        cur.execute(stmt)

    # Load data
    load_csv_data(conn, "accounts", "data/accounts.csv", [
        "customer_id", "first_name", "last_name", "address_1", "address_2",
        "city", "state", "zip_code", "join_date"
    ])

    load_csv_data(conn, "products", "data/products.csv", [
        "product_id", "product_code", "product_description"
    ])

    load_csv_data(conn, "transactions", "data/transactions.csv", [
        "transaction_id", "transaction_date", "product_id",
        "quantity", "account_id"
    ])

    # Finalize
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
