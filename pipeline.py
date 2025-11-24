import argparse
import subprocess
from pathlib import Path

from src.extract.fbref_championship import run as run_fbref
from src.extract.transfermarkt_championship import run as run_transfermarkt
from src.transform.build_semantic_2024_25 import main as run_transform
from src.load.load_curated_to_gbq import main as run_load


def extract():
    print("Extracting FBref...")
    run_fbref()
    print("Extracting Transfermarkt...")
    run_transfermarkt()


def transform():
    print("Transforming data...")
    run_transform()


def load():
    print("Loading to BigQuery...")
    run_load()


def reports():
    """
    Run the notebook reporting script located at src/notebooks/run_notebooks.py.
    """
    script_path = Path("src") / "notebooks" / "run_notebooks.py"

    if not script_path.exists():
        print("No reporting script found at src/notebooks/run_notebooks.py.")
        return

    print("Running notebook reporting script...")
    try:
        subprocess.run(["python", str(script_path)], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Notebook report generation failed: {e}")


def run_all():
    extract()
    transform()
    load()
    reports()
    print("Pipeline complete!")


def main():
    parser = argparse.ArgumentParser(description="Championship ETL pipeline runner")
    parser.add_argument(
        "stage",
        choices=["extract", "transform", "load", "reports", "all"],
        help="Which part of the pipeline to run",
    )

    args = parser.parse_args()

    if args.stage == "extract":
        extract()
    elif args.stage == "transform":
        transform()
    elif args.stage == "load":
        load()
    elif args.stage == "reports":
        reports()
    elif args.stage == "all":
        run_all()


if __name__ == "__main__":
    main()
