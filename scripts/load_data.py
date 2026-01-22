import pandas as pd
import psycopg2

# Dane logowania do bazy PostgreSQL
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydb"
DB_USER = "postgres"
DB_PASS = "postgres"

# Funkcja do załadowania danych do PostgreSQL
def load_csv_to_postgres(csv_path, table_name, conn):
    df = pd.read_csv(csv_path)
    columns = df.columns
    column_defs = ", ".join([f"{col} TEXT" for col in columns])

    cursor = conn.cursor()

    # Tworzymy tabelę dynamicznie
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    cursor.execute(f"CREATE TABLE {table_name} ({column_defs});")

    # Wstawiamy dane
    for row in df.itertuples(index=False, name=None):
        placeholders = ','.join(['%s'] * len(row))
        cursor.execute(
            f"INSERT INTO {table_name} VALUES ({placeholders})", row
        )

    conn.commit()
    cursor.close()
    print(f"Loaded data into table '{table_name}'.")

def main():
    # Połączenie z bazą
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

    # Lista plików i odpowiadających im tabel
    files_tables = {
        "data/customers.csv": "customers",
        "data/products.csv": "products",
        "data/orders.csv": "orders",
    }

    for csv_path, table_name in files_tables.items():
        load_csv_to_postgres(csv_path, table_name, conn)

    conn.close()

if __name__ == "__main__":
    main()
