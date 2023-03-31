import datetime
import requests
import re
import urllib
import os
from pathlib import Path

from dateutil import rrule


class LIS_Iterator:
    start_date = None
    end_date = None
    root_url = None
    date_format = "YYYYMMDD"

    # Default Constructor using datetime date
    def __init__(self, start_date, end_date, options):
        self.start_date = start_date
        self.end_date = end_date
        self.source_dir = options['source_dir']

    def get_range(self):
        return_obj = []
        source_dir = self.source_dir

        for dt in rrule.rrule(rrule.MONTHLY, dtstart=self.start_date.replace(day=1), until=self.end_date):
            current_year = dt.strftime('%Y')
            current_month = dt.strftime('%m')

            current_path = os.path.join(source_dir, current_year + current_month)

            for filepath in Path(current_path).glob('LIS_Africa_daily_*'):
                filename = filepath.stem
                current_date = datetime.datetime.strptime(filename.split('_')[-1].split(".")[0], '%Y%m%d')
                if self.start_date <= current_date.date() <= self.end_date:
                    return_obj.append({
                        'date': current_date.strftime('%Y%m%d'),
                        'source': filepath
                    })
                else:
                    # TODO log skipping date
                    continue

        current_path = os.path.join(source_dir, "forecasts")
        for filepath in Path(current_path).glob('LIS_Africa_daily_*'):
            filename = filepath.stem
            current_date = datetime.datetime.strptime(filename.split('_')[-1].split(".")[0], '%Y%m%d')
            if self.start_date <= current_date.date() <= self.end_date:
                return_obj.append({
                                    'date': current_date.strftime('%Y%m%d'),
                                    'source': filepath
                                })

        return return_obj
