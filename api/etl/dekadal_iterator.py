import datetime
import math

class Dekadal_Iterator():
    start_date = None
    end_date = None
    date_format = "dekadal"

    # Default Constructor using datetime date
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def get_dekad(self, date):
        dekad = (date.month - 1) * 3
        dekad += min(date.day - 1, 29) // 10 + 1
        return dekad

    def get_begin_date(self, dekad, year):
        dates = [1, 11, 21]
        day = dates[dekad % 3 - 1]
        month = math.ceil(dekad / 3.0)
        return datetime.date(year, month, day)

    def get_range(self):
        start_year = self.start_date.year
        start_dekad = self.get_dekad(self.start_date)
        end_year = self.end_date.year
        end_dekad = self.get_dekad(self.end_date)
        date_range = []

        for year in range(start_year, end_year + 1):
            for dekad in range(start_dekad if year == start_year else 1, end_dekad + 1 if year == end_year else 36):
                date_range.append(self.get_begin_date(dekad, year))

        return_obj = [{'date': date.strftime('%Y%m%d')} for date in date_range]

        return return_obj

