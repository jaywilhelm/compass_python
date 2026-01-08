from client import create_gateway_client
import compassiot.compass.v1.time_pb2 as time
import compassiot.platform.v1.unary_pb2 as unary


def main():
    client = create_gateway_client()

    request = unary.AggregateByPathRequest(
        linestring_wkt=    "LINESTRING(-82.65659331 39.68409211,-82.65479544 39.68190399,-82.65248728 39.67978823,-82.65058112 39.67869679,-82.64776913 39.67755011,-82.64486680 39.67632107,-82.64115453 39.67474288,-82.63825640 39.67363797,-82.63454725 39.67281955)",
        #"LINESTRING (151.194525 -33.87363, 151.194109 -33.873076, 151.193736 -33.872564, 151.193507 -33.872265, 151.193347 -33.872052, 151.193259 -33.871925, 151.193228 -33.871889)",
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

    response = client.AggregateByPath(request)

    print(response)


if __name__ == "__main__":
    main()
