## Summer class project

This is a stock price prediction system which is built to predict the price of the S&P stocks based on the open price, high price, low price, close price and volume of the day to predict the price of the next day. The dataset of price data of all S&P companies is loaded in the POSTGRES database and the price is predicted with the help of the ML algorithm.

There are some prerequisites before this project can be run and they are as follows:

1.  Python version must be 3.9 or higher
2.  PostgresSQL Database must be downloaded
3.  Git to download the project

Below is the manual for downloading and running project from github and it is shown in the steps below:

**Step 1:** Copy the URL of the kaggle dataset by using the link "https://storage.googleapis.com/kaggle-data-sets/1125174/4861155/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250823%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250823T122615Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=af70b789784693a99771708d612f746fb162041e1d766e588e3eaccf8915e010e57c5df5a140283c77c749a2be9425a9d0826d97c864598ace18749219e22ec627b80b466665076724b1d8cf1836cd5bddfb9fb0f8ed13f253b1055c7b8fce41f1eab1059b747a4c8954e74c4955922503553a01ac24f5feb44860df8eaa21ed1d0e3421c80c8eccbb78fbdd7036e57e0f7619fc58eb947e5f0861b9b2d527e3a245c13c756f3356a48d95fe81519d493e5f896653384fc53c8749397b70f988cb655d3081e9c7e7fc74c5dc9875fc3a64a94bddd2d60ab16bbee0bd782d0e5647bf048f85c47ab8c097475ad563aaafc89821cf769fbd3ab77e44b01a3f623b".

**Step 2:** Open your command prompt (Windows) or terminal (Mac/Linux).

**Step 3:** Go to the folder where you want to keep the project. Example: cd Desktop

**Step 4:** Clone the project from github by using the command: git clone https://github.com/LudicrousMagician/SP-500-Stock-Price-Prediction

**Step 5:** Create a virtual environment by running the command python -m venv venv and then activate it by using source venv/bin/activate

**Step 6:** Install required libraries.

**Step 7:** Open Debugger in the Visual Studio Code on the project folder and step by step run: extract, transform and load files (execute.py)

**Step 8:** After the load phase is completed the data must be loaded in the POSTGRES database

**Step 9:** Run the application by using python app.py command

**Step 10:** After running step 7 Starting server at http://127.0.0.1:8000/ this message is seen in the terminal

**Step 11:** Finally open the browser and go to http://127.0.0.1:8000/
