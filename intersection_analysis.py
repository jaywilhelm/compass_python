from client import create_gateway_client
import compassiot.compass.v1.geo_pb2 as geo
import compassiot.compass.v1.time_pb2 as time
import compassiot.platform.v1.unary_pb2 as unary


def main():
    client = create_gateway_client()

    request = unary.IntersectionRequest(
        intersection_center=geo.LatLng(
            lat=-33.87973611766243, 
            lng=151.2091378332539
        ),
        intersection_radius=26,
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
			    time.DayOfWeek.SUNDAY
            ],
            hour_of_day=[i for i in range(24)]  # 0 to 23
        )
    )

    response = client.IntersectionAnalysis(request)

    print(response)


if __name__ == "__main__":
    main()
