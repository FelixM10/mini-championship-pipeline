import argparse

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


def run_all():
    extract()
    transform()
    load()
    print("Pipeline complete!")


def main():
    parser = argparse.ArgumentParser(description="Championship ETL pipeline runner")
    parser.add_argument(
        "stage",
        choices=["extract", "transform", "load", "all"],
        help="Which part of the pipeline to run",
    )
    
    args = parser.parse_args()

    if args.stage == "extract":
        extract()
    elif args.stage == "transform":
        transform()
    elif args.stage == "load":
        load()
    elif args.stage == "all":
        run_all()


if __name__ == "__main__":
    main()
