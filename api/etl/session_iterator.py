import datetime
import requests
import re


class Link_Iterator:
    start_date = None
    end_date = None
    root_url = None
    date_format = "YYYYMMDD"

    # Default Constructor using datetime date
    def __init__(self, start_date, end_date, options):
        self.start_date = start_date
        self.end_date = end_date
        self.root_url = options['root_url']
        self.search = options['search']
        self.indexedBy = options['indexedBy']
        self.timedelta = options['timedelta']

    def get_range(self):
        dates = []
        response = requests.get(self.root_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date.replace(day=1), until=end_date):
            current_year = dt.strftime('%Y')
            current_month = dt.strftime('%m')
            response = session.get(current_text_http_path.format(current_year, current_month))
            if response.status_code != 200:
                continue
            tif_filepath_list = response.text.split()
            for tif_filepath in tif_filepath_list:
                tif_filename = tif_filepath.split('/')[-1]
                current_day = tif_filename.split('.')[4][6:8]
                current_time = tif_filename.split('.')[4][-6:]
                current_date = datetime.datetime.strptime(date, '%Y%m%d%'

                if start_date > current_date < end_date:
                    continue

        date_range_string = [date.strftime('%Y%m%d') for date in dates]

        return date_range_string
