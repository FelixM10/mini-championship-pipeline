# Mini Championship Pipeline  
*A simple ETL pipeline for the 2024–25 English Championship.*

---

## Overview

**Mini Championship Pipeline** is a modular ETL pipeline designed to extract, transform, and load club and player data for the 2024–25 Championship season.  
It uses:

- Locally saved HTML snapshots from **FBref**
- **Transfermarkt** URLs
- **Google Cloud Storage (GCS)** for object storage
- **Google BigQuery (GBQ)** for warehousing / analytics
- **Analytics Report** generated from a notebook, focused on Swansea

---

## Architecture

```
HTML Snapshots → Extract → Transform → Curated CSVs → Upload to GCS → Load to BigQuery
```

Modules:

### **src/extract/** - scraping Transfermarkt and FBRef data 

Transfermarkt tables have been extracted using requests, since the website's robots.txt allows scraping. However, in the case of FBRef, scraping is blocked (returns 403 regarless of wait time) - rather than using headless browsers or employing legally ambiguous tactics, I have decided to simply download the HTML locally and extract it from there. Since the data we scrape is not going to be updated in the future, this approach works best.

In addition to the two websites suggested in the interview task, I have scraped an additional 4 pages from FBRef for some advanced player stats to be able to have a view on a new metric - player "usage". This will be detailed in the "Advanced Stats" section below.

### **src/transform/** — cleaning, tidying, feature engineering 

Semantic tables were created, making sure to align both data sources in terms of club names, country names, and column names. An exhaustive mapping of countries and different country codes/spellings was generated as a dictionary - this can be updated to catch future instances of mismatches, but for now it covers our use case. Club names were standardised across data sources, and clubs were given a club-id and club-key. The pipeline also creates a helper file in data/utils, which does not get uploaded to GBQ, but allows the user to check current club mapping.

Five semantic tables were created: an enhanced league table that contains transfer information as well, a standard player stats table, transfers in and transfers out tables, and an advanced player stats table.

### **src/load/** — uploading to GCS, loading into BigQuery  

Using a provided service account, the user can upload these tables to a GCS bucket, and load them into a BigQuery dataset, as part of running the pipeline.

### **notebooks/** — Jupyter Notebook generating analytics report 

As part of the pipeline, a simple analytics report is generated, focused on Swansea.

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
python pipeline.py reports
```

---

## Project Structure

```
mini-championship-pipeline/
├── data/
│   ├── curated/
│   ├── raw/
│       └── html/
│   ├── transform/
│   └── utils/
├── notebooks/
├── reports/
├── src/
│   ├── extract/
│   ├── load/
│   ├── transform/
│   └── utils/
│       └── helpers/
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
Open folder using VSCode (or your preferred IDE)

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

### 4. Create your `.env` and import credentials

```bash
cp .env.example .env
```

Add the provided pipeline-sa.json file to root/credentials/

Edit `.env`:

```env
GCP_PROJECT_ID=swansea-478816
GCS_BUCKET=championship-2024-25
BQ_DATASET=championship_2024_25
GOOGLE_APPLICATION_CREDENTIALS=credentials/pipeline-sa.json
```

---

## Outputs

- **Google Cloud Storage Bucket/raw** — raw files 
- **Google Cloud Storage Bucket/curated** — curated files 
- **BigQuery dataset** — curated files uploaded as GBQ tables
- **HTML Report** - Focused on Swansea

---

## Windows Troubleshooting

This section covers common setup issues on Windows and how to fix them.
If you run into any of the following errors, follow the steps provided.

------------------------------------------------------------------------

### 1. `python` not recognized

Error:

    'python' is not recognized as an internal or external command

Cause: Python is installed, but not added to your system PATH.

Fix: Reinstall Python from https://python.org. 2. On the installer's
first screen, check: Add Python to PATH 3. Complete the installation and
restart your terminal.

------------------------------------------------------------------------

### 2. `py` works but `python` does not

If:

    py --version   works
    python --version   fails

Use `py` instead of `python`:

    py script.py

------------------------------------------------------------------------

### 3. Git not recognized

Error:

    'git' is not recognized as an internal or external command

Fix: 1. Download Git for Windows: https://git-scm.com 2. During
installation, select: Git from the command line and also from 3rd-party
software 3. Restart terminal. 4. Verify: git --version

------------------------------------------------------------------------

### 4. Cannot activate virtual environment --- "running scripts is disabled"

Error:

    .venv\Scripts\activate : File cannot be loaded because running scripts is disabled on this system.

Fix (recommended): Run PowerShell as Administrator:

    ```Set-ExecutionPolicy RemoteSigned```
    
    Press A

Then activate:

    .venv\Scripts\activate

------------------------------------------------------------------------

### 5. VS Code not using the correct Python interpreter

Fix: 1. Open VS Code. 2. Press Ctrl + Shift + P. 3. Select: Python:
Select Interpreter 4. Choose: `<project>`{=html}/venv/Scripts/python.exe

---


## Steps to run pipeline:

open VSCode (or your preferred IDE) - new terminal
```bash
git clone <repo>
cd mini-championship-pipeline
```
open Folder in VSCode - select repository folder
```
bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python pipeline.py all
```

---

## Potential future improvements

- Add p90 possession adjusted stats to both player and squad tables
- Add percentiles to create player profiles
- Add parquet file processing for larger datasets
- Expand player stats to collect more detailed information
- Add per-match stats to check performance v different opponents