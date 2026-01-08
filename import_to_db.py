import os
import configparser
from linestring_to_earth import load_linestring_from_textfile
import mariadb
from create_vehicle_telemetry_table import *
from csv_import_lib import *
from datetime import datetime

def get_db_connection(config_path="db_config.ini"):
    config = configparser.ConfigParser()
    config.read(config_path)

    db = config["mariadb"]

    return mariadb.connect(
        host=db["host"],
        port=int(db.get("port", 3306)),
        database=db["database"],
        user=db["user"],
        password=db["password"],
        autocommit=False
    )
conn = get_db_connection()

####
#
# print(drop_vehicle_telemetry_table(conn))
# print(drop_metadata_table(conn))
# #
# ####
# print(create_metadata_table(conn))
# status = recreate_table_if_empty(conn, "vehicle_telemetry")
# print(status)


def ImportDataSet(conn,
                csvfile: str,
                linestring: str,
                district: int,
                source: str):

    metadata_id = insert_metadata(
        conn,
        district=district,
        downloaded_at=datetime.now(),
        source=source,
        filename=csvfile,
        route_linestring=linestring  # whatever string format you already have
    )

    print(f"Metadata ID: {metadata_id}")
    res = import_csv(conn,csv_path=csvfile,metadata_id=metadata_id)
    print(res)
    res = verify_csv_uploaded(conn, csvfile, table_name="vehicle_telemetry")
    print(res)

ImportDataSet(conn,
                csvfile="July_2025_SB_D7_OHGO_TEST.csv",
                linestring=load_linestring_from_textfile("D7_OHGO.txt"),
                district=7,
                source="LS from OHGO")

ImportDataSet(conn,
                csvfile="July_2025_SB_D4_OHGO_TEST.csv",
                linestring=load_linestring_from_textfile("D4_OHGO.txt"),
                district=4,
                source="LS from OHGO")