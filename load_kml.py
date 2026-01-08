from pathlib import Path
import re

def load_two_linestrings_from_kml(kml_path: str):
    """
    Load exactly two LineStrings from a KML file and return them
    as WKT LINESTRING strings.

    Returns:
        (linestring1, linestring2)
    """

    text = Path(kml_path).read_text(encoding="utf-8")

    # Find ALL <coordinates> blocks (each LineString has one)
    matches = re.findall(r"<coordinates>(.*?)</coordinates>", text, re.DOTALL)

    if len(matches) != 2:
        raise ValueError(
            f"Expected exactly 2 LineStrings in KML, found {len(matches)}"
        )

    def kml_coords_to_wkt(coords_text: str) -> str:
        points = []
        for line in coords_text.strip().split():
            lon, lat, *_ = line.split(",")
            points.append(f"{lon} {lat}")
        if len(points) < 2:
            raise ValueError("LineString must contain at least two points")
        return "LINESTRING(" + ", ".join(points) + ")"

    linestring_1 = kml_coords_to_wkt(matches[0])
    linestring_2 = kml_coords_to_wkt(matches[1])

    return linestring_1, linestring_2

def load_linestring_from_kml(kml_path: str) -> str:
    """
    Load exactly ONE LineString from a KML file and return it as
    a WKT LINESTRING (lon lat, lon lat, ...).

    Raises:
        ValueError if zero or more than one LineString is found.
    """

    text = Path(kml_path).read_text(encoding="utf-8")

    # Extract all <coordinates> blocks
    matches = re.findall(r"<coordinates>(.*?)</coordinates>", text, re.DOTALL)

    if len(matches) == 0:
        raise ValueError("No LineString found in KML file")
    if len(matches) > 1:
        raise ValueError(f"Expected exactly 1 LineString, found {len(matches)}")

    coords_text = matches[0].strip()

    points = []
    for line in coords_text.split():
        lon, lat, *_ = line.split(",")
        points.append(f"{lon} {lat}")

    if len(points) < 2:
        raise ValueError("LineString must contain at least two points")

    return "LINESTRING(" + ", ".join(points) + ")"

if __name__ == "__main__":
    ls1, ls2 = load_two_linestrings_from_kml("D4_Extra.kml")
    print(ls1)
    print(ls2)