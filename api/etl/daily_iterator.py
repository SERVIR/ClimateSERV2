import datetime


class Daily_Iterator():
    start_date = None
    end_date = None
    date_format = "YYYYMMDD"

    # Default Constructor using datetime date
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def get_range(self):
        delta = self.end_date - self.start_date
        date_range = [self.start_date + datetime.timedelta(days=x) for x in range(delta.days + 1)]
        date_range_string = [date.strftime('%Y%m%d') for date in date_range]

        return date_range_string