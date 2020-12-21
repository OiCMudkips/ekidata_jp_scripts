#!python3
import argparse
import csv
import os
import sys
import sqlite3

COMPANY_CREATE_SQLITE_STATEMENT = """
    CREATE TABLE IF NOT EXISTS companies
    (
        id INTEGER PRIMARY KEY,
        railroad_id INTEGER,
        name_normal TEXT,
        name_kana TEXT,
        name_long TEXT,
        name_short TEXT,
        url TEXT,
        type INTEGER,
        status INTEGER,
        sort INTEGER
    );
"""
COMPANY_INSERT_SQLITE_STATEMENT = """
    INSERT INTO
        companies (id, railroad_id, name_normal, name_kana, name_long, name_short, url, type, status, sort)
    VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

JOIN_CREATE_SQLITE_STATEMENT = """
    CREATE TABLE IF NOT EXISTS joins
    (
        line_id INTEGER,
        station1_id INTEGER,
        station2_id INTEGER
    );
"""
JOIN_INSERT_SQLITE_STATEMENT = """
    INSERT INTO
        joins (line_id, station1_id, station2_id)
    VALUES
        (?, ?, ?);
"""

LINE_CREATE_SQLITE_STATEMENT = """
    CREATE TABLE IF NOT EXISTS lines
    (
        id INTEGER PRIMARY_KEY,
        company_id INTEGER,
        name_normal TEXT,
        name_kana TEXT,
        name_long TEXT,
        colour_hex TEXT,
        colour_text TEXT,
        line_type INTEGER,
        longitude REAL,
        latitude REAL,
        zoom TEXT,
        status INTEGER,
        sort INTEGER
    );
"""
LINE_INSERT_SQLITE_STATEMENT = """
    INSERT INTO
        lines (id, company_id, name_normal, name_kana, name_long, colour_hex, colour_text, line_type, longitude, latitude, zoom, status, sort)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

STATION_CREATE_SQLITE_STATEMENT = """
    CREATE TABLE IF NOT EXISTS stations
    (
        id INTEGER PRIMARY KEY,
        group_id INTEGER,
        name_normal TEXT,
        name_kana TEXT,
        name_romaji TEXT,
        line_id INTEGER,
        prefecture INTEGER,
        postcode TEXT,
        address TEXT,
        longitude REAL,
        latitude REAL,
        open_date TEXT,
        close_date TEXT,
        status INTEGER,
        sort INTEGER
    )
"""
STATION_INSERT_SQLITE_STATEMENT = """
    INSERT INTO
        stations (id, group_id, name_normal, name_kana, name_romaji, line_id, prefecture, postcode, address, longitude, latitude, open_date, close_date, status, sort)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

CREATE_TABLE_QUERIES = (
    COMPANY_CREATE_SQLITE_STATEMENT,
    JOIN_CREATE_SQLITE_STATEMENT,
    LINE_CREATE_SQLITE_STATEMENT,
    STATION_CREATE_SQLITE_STATEMENT,
)


def get_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert ekidata.jp files into sqlite")
    parser.add_argument("output_file", help="output path for sqllite db file")
    parser.add_argument("--companies", help="path to company*.csv")
    parser.add_argument("--joins" , help="path to join*.csv")
    parser.add_argument("--lines", help="path to line*.csv")
    parser.add_argument("--stations", help="path to station*.csv")

    return parser
    
def populate_sqlite_table(csv_file, cursor, insert_query):
    with open(csv_file, "r", newline="") as f:
        csv_reader = csv.reader(f)

        # skip headers
        next(csv_reader)

        for row in csv_reader:
            cursor.execute(insert_query, row)


def run() -> None:
    args = get_argparser().parse_args()

    if os.path.exists(args.output_file):
         raise ValueError("File already exists.")

    conn = sqlite3.connect(args.output_file)
    cursor = conn.cursor()

    for create_table_query in CREATE_TABLE_QUERIES:
        cursor.execute(create_table_query)

    csv_file_to_insert_query_map = {
        COMPANY_INSERT_SQLITE_STATEMENT: args.companies,
        JOIN_INSERT_SQLITE_STATEMENT: args.joins,
        LINE_INSERT_SQLITE_STATEMENT: args.lines,
        STATION_INSERT_SQLITE_STATEMENT: args.stations,
    }
    for insert_query, _file in csv_file_to_insert_query_map.items():
        if _file:
            populate_sqlite_table(_file, cursor, insert_query)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    run()
