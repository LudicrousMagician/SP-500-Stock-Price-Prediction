import sys
import os
import psycopg2
from pyspark.sql import SparkSession

def create_spark_session():
    """Initialize Spark session."""
    print("Initializing Spark session...")
    return SparkSession.builder.appName("SP500DataLoad").getOrCreate()

def create_postgres_tables(pg_un, pg_pw):
    """Create PostgreSQL tables if they don't exist."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=pg_un,
            password=pg_pw,
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        print("Connected to PostgreSQL database")

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS sp500_master (
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                adjusted_close FLOAT,
                volume BIGINT,
                ticker VARCHAR(10),
                daily_return FLOAT,
                ma_7 FLOAT,
                ma_20 FLOAT,
                std_20 FLOAT,
                bollinger_upper FLOAT,
                bollinger_lower FLOAT,
                rsi FLOAT,
                ema_12 FLOAT,
                ema_26 FLOAT,
                macd FLOAT
            );
            """
        ]

        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        print("PostgreSQL tables created successfully")
    
    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("PostgreSQL connection closed")

def load_to_postgres(spark, input_dir, pg_un, pg_pw):
    """Load Stage 2 and master Parquet files into PostgreSQL."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": pg_un,
        "password": pg_pw,
        "driver": "org.postgresql.Driver"
    }

    # Load the master SP500 table (Stage 3)
    master_path = os.path.join(input_dir, "master_sp500.parquet")
    if not os.path.exists(master_path):
        print(f"Error: Master Parquet file {master_path} not found.")
        return
    
    try:
        master_df = spark.read.parquet(master_path)
        master_df.write.jdbc(url=jdbc_url, table="sp500_master", mode="overwrite", properties=connection_properties)
        print("Loaded SP500 master table into PostgreSQL")
    except Exception as e:
        print(f"Error loading SP500 master table: {e}")

if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: python load/execute.py <input_dir> <pg_un> <pg_pw>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    pg_un = sys.argv[2]
    pg_pw = sys.argv[3]

    if not os.path.exists(input_dir):
        print(f"Error: Input Directory {input_dir} does not exist")
        sys.exit(1)

    spark = create_spark_session()
    create_postgres_tables(pg_un, pg_pw)
    load_to_postgres(spark, input_dir, pg_un, pg_pw)

    print("Load stage completed successfully")
