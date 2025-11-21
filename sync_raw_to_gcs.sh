#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="swansea-478816"
BUCKET="championship-2024-25-raw"

# Local paths (adjust if yours differ)
RAW_DIR="data/raw"

echo "Syncing raw CSVs from ${RAW_DIR} to gs://${BUCKET} ..."

# FBRef
gsutil cp "${RAW_DIR}/fbref_championship_player_standard_stats_2024_25.csv" \
  "gs://${BUCKET}/fbref/fbref_championship_player_standard_stats_2024_25.csv"

gsutil cp "${RAW_DIR}/fbref_championship_squad_standard_stats_2024_25.csv" \
  "gs://${BUCKET}/fbref/fbref_championship_squad_standard_stats_2024_25.csv"

# Transfermarkt
gsutil cp "${RAW_DIR}/transfermarkt_league_table_2024_25.csv" \
  "gs://${BUCKET}/transfermarkt/transfermarkt_league_table_2024_25.csv"

gsutil cp "${RAW_DIR}/transfermarkt_transfers_in_2024_25.csv" \
  "gs://${BUCKET}/transfermarkt/transfermarkt_transfers_in_2024_25.csv"

gsutil cp "${RAW_DIR}/transfermarkt_transfers_out_2024_25.csv" \
  "gs://${BUCKET}/transfermarkt/transfermarkt_transfers_out_2024_25.csv"

echo "Done."
