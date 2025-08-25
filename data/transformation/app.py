from flask import Flask, render_template, request
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel
import os

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, "models")
spark = SparkSession.builder.appName("SP500App").getOrCreate()
_MODEL_CACHE = {}

def get_available_tickers():
    if not os.path.isdir(MODELS_DIR):
        return []
    return sorted([f.replace("_model","") for f in os.listdir(MODELS_DIR) if f.endswith("_model")])

def load_pipeline_model_for_ticker(ticker):
    if ticker in _MODEL_CACHE:
        return _MODEL_CACHE[ticker]
    model_path = os.path.join(MODELS_DIR, f"{ticker}_model")
    if not os.path.isdir(model_path):
        raise FileNotFoundError(f"Model not found: {model_path}")
    model = PipelineModel.load(model_path)
    _MODEL_CACHE[ticker] = model
    return model

@app.route("/", methods=["GET", "POST"])
def index():
    tickers = get_available_tickers()
    prediction = None
    prediction_color = None
    error = None

    # Default form values
    form_data = {"ticker": "", "open": "", "high": "", "low": "", "close": "", "volume": ""}

    if request.method == "POST":
        try:
            form_data["ticker"] = request.form["ticker"]
            form_data["open"] = request.form["open"]
            form_data["high"] = request.form["high"]
            form_data["low"] = request.form["low"]
            form_data["close"] = request.form["close"]
            form_data["volume"] = request.form["volume"]

            open_val = float(form_data["open"])
            high_val = float(form_data["high"])
            low_val = float(form_data["low"])
            close_val = float(form_data["close"])
            volume_val = float(form_data["volume"])

            model = load_pipeline_model_for_ticker(form_data["ticker"])
            input_df = spark.createDataFrame([Row(open=open_val, high=high_val, low=low_val, close=close_val, volume=volume_val)])
            output_df = model.transform(input_df)
            predicted_price_val = float(output_df.select("prediction").collect()[0][0])

            diff = predicted_price_val - close_val
            prediction_sign = '+' if diff >= 0 else '-'
            prediction = f"{predicted_price_val:.2f} ({prediction_sign}{abs(diff):.2f})"
            prediction_color = "green" if prediction_sign == "+" else "red"

        except Exception as e:
            error = str(e)

    return render_template("form.html", tickers=tickers, prediction=prediction, prediction_color=prediction_color, error=error, form_data=form_data)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
