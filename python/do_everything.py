from processed_point_by_geometry import store_datapull
from linestring_to_earth import load_linestring_from_textfile
import compassiot.compass.v1.time_pb2 as time


date_range = time.DateTimeRange(
        start=time.LocalDate(
            day=1,
            month=3,
            year=2025
        ),
        end=time.LocalDate(
            day=31,
            month=3,
            year=2025
        ),
        # day_of_week=[
        #     time.DayOfWeek.SATURDAY,
        #     time.DayOfWeek.SUNDAY,
        #     time.DayOfWeek.MONDAY,
        #     time.DayOfWeek.TUESDAY,
        # ],
        hour_of_day=[i for i in range(24)],
    )


linestring = load_linestring_from_textfile("D4_OHGO.txt")
filename = "March_2025_SB_D4_OHGO_TEST"

store_datapull(linestring,date_range,filename)

linestring = load_linestring_from_textfile("D7_OHGO.txt")
filename = "March_2025_SB_D7_OHGO_TEST"

store_datapull(linestring,date_range,filename)
