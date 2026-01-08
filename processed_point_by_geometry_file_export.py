from client import create_gateway_client, get_enum_str

import grpc
import compassiot.gateway.v1.gateway_pb2_grpc as gateway
import compassiot.compass.v1.time_pb2 as time
import compassiot.platform.v1.streaming_pb2 as streaming
import compassiot.compass.v1.vehicle_pb2 as vehicle
import compassiot.compass.v1.file_pb2 as file

def main():
    client = create_gateway_client()

    request = streaming.ProcessedPointByGeometryFileExportRequest(
        linestring_or_polygon_wkt="LINESTRING(151.18703722810923 -33.8695894847834,151.18361472940623 -33.868689747746664)",
        date_time_range=time.DateTimeRange(
            start=time.LocalDate(
                day=1,
                month=10,
                year=2023
            ),
            end=time.LocalDate(
                day=31,
                month=10,
                year=2023
            ),
            day_of_week=[
                time.DayOfWeek.SATURDAY,
                time.DayOfWeek.SUNDAY,
                time.DayOfWeek.MONDAY,
                time.DayOfWeek.TUESDAY,
            ],
            hour_of_day=[i for i in range(24)],  # 0 to 23
            # include the list of dates you would like to exclude from the query, if any
            exclude_date=[
                time.LocalDate(
                    day=10,
                    month=10,
                    year=2023
                ),
                time.LocalDate(
                    day=11,
                    month=10,
                    year=2023
                ),
            ]
        ),
        # specify if any filter exists, leave empty for all data
        filters=[streaming.RawRequestFilter.UNSPECIFIED],
        # specify all vehicle types that you would like to receive in the response, leave empty for all vehicle types
        vehicle_type_filter=[vehicle.VehicleType.CAR, vehicle.VEHICLE_TYPE_UNSPECIFIED, vehicle.VehicleType.BUS,
        vehicle.VehicleType.HV, vehicle.VehicleType.VAN, vehicle.VehicleType.TRUCK, vehicle.VehicleType.LCV,
        vehicle.VehicleType.LV, vehicle.VehicleType.CRANE, vehicle.VehicleType.TRACTOR,
        vehicle.VehicleType.TRAILER, vehicle.VehicleType.LOADER, vehicle.VehicleType.MOTORCYCLE,
        vehicle.VehicleType.GARBAGE_TRUCK, vehicle.VehicleType.MICROBUS, vehicle.VehicleType.SUV,
        vehicle.VehicleType.LOADER, vehicle.VehicleType.PICKUP_TRUCK],
        file_type = file.CSV

    )

    response = client.ProcessedPointByGeometryFileExport(request)
    print(response)


if __name__ == "__main__":
    main()
