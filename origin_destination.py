from client import create_gateway_client
import compassiot.compass.v1.time_pb2 as time
import compassiot.platform.v1.unary_pb2 as unary


def main():
    client = create_gateway_client()

    request = unary.OriginDestinationRequest(
        selection=[
            unary.Selection(
                name="Sydney",
                wkt="POLYGON ((151.21229795582838 -33.89101682624773, 151.19573365832508 -33.87414013405557, 151.20665103622446 -33.858041660571004, 151.22490940960853 -33.870545592545284, 151.21229795582838 -33.89101682624773))",
                selection_type=unary.SelectionType.POLYGON_PASSING_THROUGH
            ),
            unary.Selection(
                name="Harris St - Broadway",
                wkt="LINESTRING (151.200496 -33.881089, 151.200734 -33.881501, 151.200777 -33.881576, 151.200816 -33.881643, 151.200949 -33.881873, 151.201152 -33.882205, 151.201216 -33.88231, 151.201246 -33.882361, 151.201583 -33.882933, 151.201817 -33.883331, 151.202152 -33.883868, 151.202201 -33.883954, 151.202245 -33.884039, 151.202118 -33.884076, 151.202098 -33.884081, 151.201749 -33.884177, 151.201519 -33.884235, 151.20132 -33.884264, 151.201233 -33.88427, 151.200789 -33.884276, 151.200213 -33.884302, 151.200078 -33.884304, 151.199982 -33.884309, 151.199922 -33.884311, 151.199824 -33.884315)",
                selection_type=unary.SelectionType.LINESTRING_PARTIAL_MATCH
            )
        ],
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

    response = client.OriginDestination(request)

    print(response)


if __name__ == "__main__":
    main()
