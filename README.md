# Mini Championship Pipeline  
*A simple ETL pipeline for the 2024–25 English Championship.*

---

## Overview

**Mini Championship Pipeline** is a modular ETL pipeline designed to extract, transform, and load club and player data for the 2024–25 Championship season.  
It uses:

- Locally saved HTML snapshots from **FBref** and **Transfermarkt**
- Clean, reproducible parsing and feature engineering
- **Google Cloud Storage (GCS)** for object storage
- **Google BigQuery (GBQ)** for warehousing / analytics

---

## Architecture

```
HTML Snapshots → Extract → Transform → Curated CSVs → Upload to GCS → Load to BigQuery
```

Modules:

### **src/extract/** - scraping Transfermarkt and FBRef data 

Transfermarkt tables have been extracted using requests, since the website's robots.txt allows scraping. However, in the case of FBRef, scraping is blocked - rather than using headless browsers or employing legally ambiguous tactics, I have decided to simply download the HTML locally and extract it from there. Since the data we scrape is not going to be updated in the future, this approach works best.

In addition to the two websites suggested in the interview task, I have scraped an additional 4 pages from FBRef for some advanced player stats to be able to have a view on a new metric - player "usage". This will be detailed in the "Advanced Stats" section below.

### **src/transform/** — cleaning, tidying, feature engineering 

Semantic tables were created, making sure to align both data sources in terms of club names, country names, and column names. An exhaustive mapping of countries and different country codes/spellings was generated as a dictionary - this can be updated to catch future instances of mismatches, but for now it covers our use case. Club names were standardised across data sources, and clubs were given a club-id and club-key. The pipeline also creates a helper file in data/utils, which does not get uploaded to GBQ, but allows the user to check current club mapping.

Five semantic tables were created: an enhanced league table that contains transfer information as well, a standard player stats table, transfers in and transfers out tables, and an advanced player stats table.

### **src/load/** — uploading to GCS, loading into BigQuery  

Using a provided service account, the user can upload these tables to a GCS bucket, and load them into a BigQuery dataset, as part of running the pipeline.

### **pipeline.py** — orchestrator for running all stages  

To run, make sure all installation instructions below have been followed, the `.venv` has been activated, and `requirements.txt` has been installed. After that, you can:

Run the full pipeline:
```bash
python pipeline.py all
```

Or run individual steps:
```bash
python pipeline.py extract
python pipeline.py transform
python pipeline.py load
```

---

## Project Structure

```
mini-championship-pipeline/
├── data/
│   ├── curated/
│   ├── raw/
│   ├── transform/
│   └── utils/
├── notebooks/
├── reports/
├── src/
│   ├── extract/
│   ├── transform/
│   └── load/
├── .env.example
├── pipeline.py
├── config.py
├── requirements.txt
└── README.md
```

---

## Getting Started

### 0. Install Python (if not already installed)

This project requires **Python 3.10 or higher** (developed using Python 3.13).

Check if Python is installed:

```bash
python3 --version
```

If Python is not installed, download it from:

https://www.python.org/downloads/

Ensure that Python is added to your PATH during installation (Windows will show a checkbox for this).

---

### 1. Clone the repository
```bash
git clone https://github.com/FelixM10/mini-championship-pipeline.git
cd mini-championship-pipeline
```

### 2. Create and activate a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate   # macOS / Linux
# On Windows:
# .venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

---

## Configuration

### 4. Create your `.env`
```bash
cp .env.example .env
```

Edit `.env`:

```env
GCP_PROJECT_ID=swansea-478816
GCS_BUCKET=championship-2024-25
BQ_DATASET=championship_2024_25
GOOGLE_APPLICATION_CREDENTIALS=credentials/pipeline-sa.json
```

---

## Outputs

- **data/raw/** — Raw HTML + parsed CSVs  
- **data/curated/** — Clean, tidied CSVs  
- **Google Cloud Storage** — Final curated files  
- **BigQuery dataset** — Analytics-ready tables  

---

## Troubleshooting

### `import dotenv could not be resolved`
Select the correct interpreter in VS Code:
```
CMD + Shift + P → Python: Select Interpreter → choose .venv/bin/python
```

### Dataset not found
Ensure the dataset name in `.env` matches exactly.

---

## For Interviewers

Steps to run:

```bash
git clone <repo>
cd mini-championship-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python pipeline.py all
```

---
