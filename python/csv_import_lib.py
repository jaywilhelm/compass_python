import csv
import mariadb
from halo import Halo
from typing import Optional,List, Dict, Any
import sys
import re

TELEMETRY_COLUMNS = [
    "vehicle_type",
    "timestamp_seconds",
    "timestamp_nanos",
    "road_matched_point_lat",
    "road_matched_point_lon",
    "speed_kmh",
    "osm_way_id",
    "vehicle_id",
    "trip_id",
    "raw_point_lat",
    "raw_point_lon",
    "transport_type",
    "acceleration_x",
    "acceleration_y",
    "acceleration_z",
    "gyro_roll",
    "gyro_pitch",
    "gyro_yaw",
    "iri",
    "near_miss_timestamp_seconds",
    "near_miss_timestamp_nanos",
    "near_miss_type",
    "bearing",
    "point_id",
]

def import_csv(
    conn,
    csv_path: str,
    *,
    table_name: str = "vehicle_telemetry",
    metadata_id: int,
    batch_size: int = 2000,
):
    """
    Import telemetry CSV into MariaDB, attaching metadata_id to every row.
    """

    columns = TELEMETRY_COLUMNS + ["metadata_id"]

    insert_sql = f"""
    INSERT IGNORE INTO `{table_name}` ({",".join(columns)})
    VALUES ({",".join(["%s"] * len(columns))})
    """

    # Optional Halo spinner
    spinner = None
    use_spinner = sys.stderr.isatty()
    if use_spinner:
        try:
            from halo import Halo
            spinner = Halo(text="Importing CSV…", spinner="dots", stream=sys.stderr)
            spinner.start()
        except Exception:
            spinner = None
            use_spinner = False

    def set_status(msg: str):
        if spinner:
            spinner.text = msg

    def succeed(msg: str):
        if spinner:
            spinner.succeed(msg)

    cur = conn.cursor()
    batch = []
    processed = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample)
        reader = csv.DictReader(f, dialect=dialect)

        missing = [c for c in TELEMETRY_COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")

        for row in reader:
            processed += 1

            values = []
            for col in TELEMETRY_COLUMNS:
                v = row.get(col)
                values.append(None if v in ("", None) else v)

            # ✅ Attach metadata_id
            values.append(int(metadata_id))

            batch.append(values)

            if processed % 5000 == 0:
                set_status(f"Processed {processed:,} rows")

            if len(batch) >= batch_size:
                cur.executemany(insert_sql, batch)
                conn.commit()
                batch.clear()

        if batch:
            cur.executemany(insert_sql, batch)
            conn.commit()
            batch.clear()

    succeed(f"Import complete: {processed:,} rows (metadata_id={metadata_id})")
    return {
        "processed_rows": processed,
        "metadata_id": metadata_id,
    }
   
def linestring_text_from_points(points_latlon):
    """
    points_latlon: iterable of (lat, lon) tuples
    Returns: WKT string like "LINESTRING(lon lat, lon lat, ...)"
    """
    pts = list(points_latlon)
    if len(pts) < 2:
        raise ValueError("LINESTRING requires at least 2 points")

    coord_parts = []
    for lat, lon in pts:
        coord_parts.append(f"{float(lon)} {float(lat)}")

    return "LINESTRING(" + ", ".join(coord_parts) + ")"     

def insert_metadata(
    conn,
    *,
    district: int,
    downloaded_at,        # datetime.datetime or "YYYY-MM-DD HH:MM:SS"
    source: str,
    route_linestring: str,  # WKT LINESTRING string
    filename:str,
    table_name: str = "import_metadata",
    srid: int = 4326,
) -> int:
    """
    Insert one metadata row.
    route_linestring must be a WKT LINESTRING string.
    """

    wkt = normalize_linestring_wkt(route_linestring)

    sql = f"""
    INSERT INTO `{table_name}` (district, downloaded_at, source, route_text, route,filename)
    VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, %s), %s)
    """

    cur = conn.cursor()
    cur.execute(
        sql,
        (
            int(district),
            downloaded_at,
            source,
            route_linestring,
            wkt,
            srid,
            filename,
        ),
    )
    conn.commit()
    return cur.lastrowid

def normalize_linestring_wkt(raw_linestring):
    """
    Normalize a LINESTRING WKT string for MariaDB insertion.

    Accepts:
      - string
      - single-element tuple containing a string (common DB fetch case)

    Returns:
      - normalized WKT string
    """

    # ---- FIX: unwrap tuple if needed ----
    if isinstance(raw_linestring, tuple):
        if len(raw_linestring) != 1:
            raise TypeError(
                f"Expected single-element tuple for LINESTRING, got {raw_linestring}"
            )
        raw_linestring = raw_linestring[0]

    if not isinstance(raw_linestring, str):
        raise TypeError(
            f"LINESTRING must be a string, got {type(raw_linestring).__name__}"
        )

    s = raw_linestring.strip()

    if not s:
        raise ValueError("Empty LINESTRING input")

    if not s.upper().startswith("LINESTRING"):
        raise ValueError(f"Input is not a LINESTRING WKT: {s[:50]}")

    m = re.search(r"LINESTRING\s*\((.*)\)", s, re.IGNORECASE | re.DOTALL)
    if not m:
        raise ValueError("Malformed LINESTRING (missing parentheses)")

    body = m.group(1).strip()
    if not body:
        raise ValueError("LINESTRING has no coordinates")

    points = []
    for idx, part in enumerate(body.split(","), start=1):
        coords = part.strip().split()
        if len(coords) != 2:
            raise ValueError(f"Invalid coordinate pair at position {idx}: '{part}'")

        try:
            lon = float(coords[0])
            lat = float(coords[1])
        except ValueError:
            raise ValueError(f"Non-numeric coordinate at position {idx}: '{part}'")

        points.append(f"{lon} {lat}")

    if len(points) < 2:
        raise ValueError("LINESTRING must contain at least two points")

    return "LINESTRING(" + ", ".join(points) + ")"


def verify_csv_uploaded(
    conn,
    csv_path: str,
    *,
    table_name: str = "vehicle_telemetry",
    batch_size: int = 1000,
    missing_sample_limit: int = 1,
):
    """
    Verify CSV upload by querying MariaDB in batches of point_ids (default 100).

    Does NOT:
      - download all point_ids from DB
      - use temp tables
      - use sqlite

    Returns dict:
      - csv_rows_total
      - ids_checked (after per-batch dedupe)
      - matched_count
      - missing_count
      - missing_sample
      - ok
    """

    # Optional Halo spinner (fallback silently if unavailable / not a TTY)
    spinner = None
    use_spinner = sys.stderr.isatty()
    if use_spinner:
        try:
            from halo import Halo
            spinner = Halo(text="Starting batch verification…", spinner="dots", stream=sys.stderr)
            spinner.start()
        except Exception:
            spinner = None
            use_spinner = False

    def set_status(msg: str):
        if use_spinner and spinner:
            spinner.text = msg

    def succeed(msg: str):
        if use_spinner and spinner:
            spinner.succeed(msg)

    def fail(msg: str):
        if use_spinner and spinner:
            spinner.fail(msg)

    def check_batch(point_ids: List[str]) -> (int, List[str]):
        """
        Returns:
          matched_in_batch_count (based on unique ids in batch)
          missing_ids_in_batch
        """
        # Deduplicate within the batch to avoid giant IN lists with repeats
        unique_ids = list(dict.fromkeys(point_ids))  # preserves order

        placeholders = ",".join(["%s"] * len(unique_ids))
        sql = f"SELECT point_id FROM `{table_name}` WHERE point_id IN ({placeholders})"

        cur = conn.cursor()
        cur.execute(sql, tuple(unique_ids))
        found = {row[0] for row in cur.fetchall()}

        missing = [pid for pid in unique_ids if pid not in found]
        matched = len(unique_ids) - len(missing)
        return matched, missing

    csv_rows_total = 0
    ids_checked = 0
    matched_count = 0
    missing_count = 0
    missing_sample: List[str] = []

    batch: List[str] = []
    batches_done = 0

    try:
        set_status("Reading CSV…")
        with open(csv_path, newline="", encoding="utf-8") as f:
            sample = f.read(4096)
            f.seek(0)
            dialect = csv.Sniffer().sniff(sample)
            reader = csv.DictReader(f, dialect=dialect)

            if "point_id" not in (reader.fieldnames or []):
                raise ValueError(f"'point_id' column not found in CSV header: {reader.fieldnames}")

            for row in reader:
                csv_rows_total += 1
                pid = row.get("point_id")

                if pid in (None, ""):
                    raise ValueError(f"Empty point_id encountered at CSV row {csv_rows_total}")

                batch.append(pid)

                if len(batch) >= batch_size:
                    batches_done += 1
                    set_status(
                        f"Checking batch {batches_done:,} | CSV rows {csv_rows_total:,} | "
                        f"matched {matched_count:,} | missing {missing_count:,}"
                    )

                    matched, missing = check_batch(batch)

                    # ids_checked counts unique IDs per batch
                    ids_checked += len(set(batch))
                    matched_count += matched
                    missing_count += len(missing)

                    if missing and len(missing_sample) < missing_sample_limit:
                        space = missing_sample_limit - len(missing_sample)
                        missing_sample.extend(missing[:space])

                    batch.clear()

            # Flush last partial batch
            if batch:
                batches_done += 1
                set_status(
                    f"Checking final batch {batches_done:,} | CSV rows {csv_rows_total:,} | "
                    f"matched {matched_count:,} | missing {missing_count:,}"
                )

                matched, missing = check_batch(batch)
                ids_checked += len(set(batch))
                matched_count += matched
                missing_count += len(missing)

                if missing and len(missing_sample) < missing_sample_limit:
                    space = missing_sample_limit - len(missing_sample)
                    missing_sample.extend(missing[:space])

                batch.clear()

        ok = (missing_count == 0)

        result: Dict[str, Any] = {
            "csv_rows_total": csv_rows_total,
            "ids_checked": ids_checked,            # unique-per-batch count (not global unique)
            "matched_count": matched_count,        # based on unique-per-batch
            "missing_count": missing_count,
            "missing_sample": missing_sample,
            "ok": ok,
        }

        if ok:
            succeed(
                f"Verified OK: no missing point_ids. "
                f"CSV rows {csv_rows_total:,} | checked {ids_checked:,} (unique-per-batch)."
            )
        else:
            fail(
                f"Verification FAILED: missing {missing_count:,} point_ids. "
                f"CSV rows {csv_rows_total:,} | checked {ids_checked:,} (unique-per-batch)."
            )

        return result

    except Exception as e:
        if use_spinner and spinner:
            spinner.fail(f"Verification failed: {e}")
        raise