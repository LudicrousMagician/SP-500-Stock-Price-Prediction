import os
import sys
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline, PipelineModel

def create_spark_session():
    return SparkSession.builder.appName("SP500Transform").getOrCreate()

def load_sp500_csv_files(base_dir, output_dir):
    sp500_dir = os.path.join(base_dir, "sp500", "csv")
    stage1_dir = os.path.join(output_dir, "stage1")
    os.makedirs(stage1_dir, exist_ok=True)

    csv_files = glob.glob(os.path.join(sp500_dir, "*.csv"))
    if not csv_files:
        print(f"No CSV files found in {sp500_dir}")
        return {}

    dataframes = {}
    for file in csv_files:
        ticker = os.path.splitext(os.path.basename(file))[0]
        df = spark.read.option("header", True).option("inferSchema", True).csv(file)
        df = df.withColumn("ticker", F.lit(ticker))
        df = df.dropDuplicates()
        df.write.mode("overwrite").parquet(os.path.join(stage1_dir, ticker))
        dataframes[ticker] = df
        print(f"Cleaned {file} saved as {ticker}")
    return dataframes

def train_or_load_model(df, ticker, models_dir):
    os.makedirs(models_dir, exist_ok=True)
    model_path = os.path.join(models_dir, f"{ticker}_model")

    if os.path.exists(model_path):
        print(f"Loading existing model for {ticker}")
        return PipelineModel.load(model_path)

    print(f"Training new model for {ticker}")
    feature_cols = ["open", "high", "low", "volume"]  # only OHLC + volume

    # Fill nulls
    for col_name in feature_cols + ["close"]:
        df = df.withColumn(col_name, F.when(F.col(col_name).isNull(), 0).otherwise(F.col(col_name)))

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="close")
    pipeline = Pipeline(stages=[assembler, lr])

    model = pipeline.fit(df)
    model.write().overwrite().save(model_path)
    print(f"Model saved for {ticker} at {model_path}")
    return model

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python execute.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session()
    dataframes = load_sp500_csv_files(input_dir, output_dir)

    models_dir = os.path.join(output_dir, "models")
    for ticker, df in dataframes.items():
        train_or_load_model(df, ticker, models_dir)

    print("Pipeline completed. Models trained/loaded for each stock.")
