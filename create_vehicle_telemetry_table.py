import mariadb

def table_exists(conn, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = %s
        LIMIT 1
        """,
        (table_name,)
    )
    return cur.fetchone() is not None


def table_has_rows(conn, table_name: str) -> bool:
    # Only call this if the table exists.
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM `{table_name}` LIMIT 1")
    return cur.fetchone() is not None

def create_metadata_table(conn, table_name="import_metadata"):
    """
    Creates a metadata table for each CSV import.
    Stores a route geometry as LINESTRING in SRID 4326.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        metadata_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        district SMALLINT NOT NULL,
        downloaded_at DATETIME NOT NULL,
        source TEXT NOT NULL,
        filename TEXT NOT NULL,
        route LINESTRING NOT NULL,
        route_text TEXT NOT NULL,
        PRIMARY KEY (metadata_id),
        INDEX idx_district_time (district, downloaded_at),
        INDEX idx_source (source)
    ) ENGINE=InnoDB
      DEFAULT CHARSET=utf8mb4
      COLLATE=utf8mb4_unicode_ci;
    """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    
def create_vehicle_telemetry_table(conn, table_name="vehicle_telemetry"):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        point_id VARCHAR(128) NOT NULL,

        vehicle_type TINYINT NOT NULL,

        timestamp_seconds BIGINT NOT NULL,
        timestamp_nanos INT NOT NULL,

        road_matched_point_lat DOUBLE NOT NULL,
        road_matched_point_lon DOUBLE NOT NULL,

        speed_kmh FLOAT NOT NULL,

        osm_way_id BIGINT,

        vehicle_id VARCHAR(128) NOT NULL,
        trip_id VARCHAR(64) NOT NULL,

        raw_point_lat DOUBLE NOT NULL,
        raw_point_lon DOUBLE NOT NULL,

        transport_type TINYINT NOT NULL,

        acceleration_x FLOAT,
        acceleration_y FLOAT,
        acceleration_z FLOAT,

        gyro_roll FLOAT,
        gyro_pitch FLOAT,
        gyro_yaw FLOAT,

        iri FLOAT,

        near_miss_timestamp_seconds BIGINT,
        near_miss_timestamp_nanos INT,
        near_miss_type TINYINT,

        bearing SMALLINT,
        metadata_id BIGINT UNSIGNED NOT NULL,
        
        PRIMARY KEY (point_id),

        INDEX idx_time (timestamp_seconds, timestamp_nanos),
        INDEX idx_vehicle (vehicle_id),
        INDEX idx_trip (trip_id),
        INDEX idx_osm_way (osm_way_id),
        INDEX idx_lat_lon (road_matched_point_lat, road_matched_point_lon)
    ) ENGINE=InnoDB
      DEFAULT CHARSET=utf8mb4
      COLLATE=utf8mb4_unicode_ci;
    """
    cur = conn.cursor()
    cur.execute(create_sql)
    conn.commit()


def recreate_table_if_empty(
    conn,
    table_name="vehicle_telemetry",
    *,
    drop_if_exists: bool = True
) -> dict:
    """
    If table doesn't exist -> create it.
    If table exists and has rows -> do nothing.
    If table exists and is empty -> drop+recreate (or just ensure schema).
    Returns a small status dict describing what happened.
    """
    exists = table_exists(conn, table_name)
    if not exists:
        create_vehicle_telemetry_table(conn, table_name)
        return {"table": table_name, "action": "created", "reason": "did_not_exist"}

    has_rows = table_has_rows(conn, table_name)
    if has_rows:
        return {"table": table_name, "action": "kept", "reason": "already_has_rows"}

    # Exists but empty
    cur = conn.cursor()
    if drop_if_exists:
        cur.execute(f"DROP TABLE `{table_name}`")
        conn.commit()
        create_vehicle_telemetry_table(conn, table_name)
        return {"table": table_name, "action": "recreated", "reason": "exists_but_empty"}
    else:
        # Just ensure schema exists (it does) and leave it.
        create_vehicle_telemetry_table(conn, table_name)
        return {"table": table_name, "action": "kept", "reason": "exists_but_empty_no_drop"}

def drop_metadata_table(conn) -> dict:
    return drop_table(conn, table_name = "import_metadata")

def drop_vehicle_telemetry_table(conn) -> dict:
    return drop_table(conn, table_name = "vehicle_telemetry")

def drop_table(conn,table_name) -> dict:
    
    """
    Unconditionally drop a table if it exists.
    Data presence does NOT matter.

    Returns a status dict for logging.
    """
    cur = conn.cursor()

    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = %s
        LIMIT 1
        """,
        (table_name,)
    )

    if cur.fetchone() is None:
        return {
            "table": table_name,
            "action": "noop",
            "reason": "does_not_exist"
        }

    cur.execute(f"DROP TABLE `{table_name}`")
    conn.commit()

    return {
        "table": table_name,
        "action": "dropped",
        "reason": "forced"
    }

def metadata_filename_exists(
    conn,
    filename: str,
    table_name: str = "import_metadata",
) -> bool:
    """
    Returns True if filename already exists in the metadata table.
    """
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT 1
        FROM `{table_name}`
        WHERE filename = %s
        LIMIT 1
        """,
        (filename,),
    )
    return cur.fetchone() is not None