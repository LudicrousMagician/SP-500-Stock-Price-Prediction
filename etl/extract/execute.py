import os, sys, requests
from zipfile import ZipFile

def download_zip_file(url, output_dir):
    """Downloads a zip file from a URL."""
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)
    if response.status_code == 200:
        filename = os.path.join(output_dir, "stock_data.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded zip file: {filename}")
        return filename
    else:
        raise Exception(f"Failed to download the file. Status code {response.status_code}")

def extract_zip_file(zip_filename, output_dir):
    """Extracts a zip file to the specified directory and deletes the zip."""
    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)
    print(f"Extracted files written to: {output_dir}")
    print("Removing the zip file...")
    os.remove(zip_filename)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Extraction path is required")
        print("Example Usage:")
        print("python3 execute.py /Users/username/Data/StockExtraction")
    else:
        try:
            print("Starting Stock Data Extraction Engine...")
            EXTRACT_PATH = sys.argv[1]
            
            # Kaggle direct download URL for your dataset
            KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1125174/4861155/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250823%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250823T122615Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=af70b789784693a99771708d612f746fb162041e1d766e588e3eaccf8915e010e57c5df5a140283c77c749a2be9425a9d0826d97c864598ace18749219e22ec627b80b466665076724b1d8cf1836cd5bddfb9fb0f8ed13f253b1055c7b8fce41f1eab1059b747a4c8954e74c4955922503553a01ac24f5feb44860df8eaa21ed1d0e3421c80c8eccbb78fbdd7036e57e0f7619fc58eb947e5f0861b9b2d527e3a245c13c756f3356a48d95fe81519d493e5f896653384fc53c8749397b70f988cb655d3081e9c7e7fc74c5dc9875fc3a64a94bddd2d60ab16bbee0bd782d0e5647bf048f85c47ab8c097475ad563aaafc89821cf769fbd3ab77e44b01a3f623b"
            
            zip_filename = download_zip_file(KAGGLE_URL, EXTRACT_PATH)
            extract_zip_file(zip_filename, EXTRACT_PATH)
            
            print("Extraction completed successfully!!!")
        except Exception as e:
            print(f"Error: {e}")
