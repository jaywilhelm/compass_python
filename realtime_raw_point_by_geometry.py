from client import create_gateway_client, retry_stream, SHORT_TIMEOUT_SEC
import compassiot.platform.v1.streaming_pb2 as streaming


def main():
    client = create_gateway_client()

    request = streaming.RealtimeRawPointByGeometryRequest(
        bounds_wkt="POLYGON ((151.24515599751396 -33.99690664408569, 151.1312304506755 -33.922084606957064, 151.16130001880038 -33.84614157217645, 151.25574387361428 -33.86302364348595, 151.2887780470458 -33.850010674904205, 151.24515599751396 -33.99690664408569))",
        stream_env= streaming.StreamEnvironment.DEV
    )
    
    for response in retry_stream(lambda: client.RealtimeRawPointByGeometry(request, timeout=SHORT_TIMEOUT_SEC)):
        print(response)


if __name__ == "__main__":
    main()