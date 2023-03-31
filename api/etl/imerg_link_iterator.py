import datetime
import requests
import re
import urllib

from bs4 import BeautifulSoup

from dateutil import rrule


class IMERG_Link_Iterator:
    start_date = None
    end_date = None
    root_url = None
    date_format = "YYYYMMDD"

    # Default Constructor using datetime date
    def __init__(self, start_date, end_date, options):
        self.start_date = start_date
        self.end_date = end_date
        self.root_url = options['root_url']
        self.root_search_url = options['root_search_url']
        self.session_user = options['session_user']
        self.session_pass = options['session_pass']

    def get_range(self):
        return_obj = []
        session = requests.Session()
        session.auth = (self.session_user, self.session_pass)

        for dt in rrule.rrule(rrule.MONTHLY, dtstart=self.start_date.replace(day=1), until=self.end_date):
            current_year = dt.strftime('%Y')
            current_month = dt.strftime('%m')
            response = session.get(self.root_search_url.format(current_year, current_month))
            if response.status_code != 200:
                # TODO LOG ERROR
                continue
            tif_filepath_list = response.text.split()
            for tif_filepath in tif_filepath_list:
                tif_filename = tif_filepath.split('/')[-1]
                current_day = tif_filename.split('.')[4][6:8]
                current_time = tif_filename.split('.')[4].split('-')[1][1:]
                if '1day' in tif_filename:
                    current_time = '000000'
                version = tif_filename.split('.')[6]
                remote_path = urllib.parse.urljoin(self.root_url, tif_filepath)
                current_date = datetime.datetime.strptime(current_year + current_month + current_day + current_time, '%Y%m%d%H%M%S')

                if self.start_date <= current_date.date() <= self.end_date:
                    return_obj.append({
                                        'date': current_date.strftime('%Y%m%d%H%M%S'),
                                        'source': remote_path,
                                        'time': current_time,
                                        'version': version,
                                        'filename': tif_filename
                                    })
                else:
                    # TODO log skipping date
                    continue

        return return_obj