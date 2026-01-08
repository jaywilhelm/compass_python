from client import create_gateway_client
from compassiot.gateway.v1.gateway_pb2 import RoadMatchPathRequest, RoadMatchPathResponse
from compassiot.compass.v1.geo_pb2 import LatLng


def main():
    client = create_gateway_client()

    # List of raw unmatched points are given as input to the API
    request = RoadMatchPathRequest(
        raw_points=[
            LatLng(lat= -33.85715882779781, lng=151.20751220989666),
            LatLng(lat= -33.84842568559147, lng=151.212589825605),
                    ]
    )

    response = client.RoadMatchPath(request)

    print(response)


if __name__ == "__main__":
    main()
