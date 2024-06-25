import argparse
from parser.parsing import load_manifest

from database.db import create_db, insert_manifest_data, query_db


def main():
    create_db()
    manifest = load_manifest("/home/valcer/projects/dbt_cli_user/manifest.json")
    insert_manifest_data(manifest)
    print("Manifest data inserted.")
    query_db()


if __name__ == "__main__":
    main()
