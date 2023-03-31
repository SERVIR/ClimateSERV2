import datetime
import requests
import re

from bs4 import BeautifulSoup


class ESI_Iterator:
    start_date = None
    end_date = None
    root_url = None
    date_format = "YYYYMMDD"

    # Default Constructor using datetime date
    def __init__(self, start_date, end_date, options):
        self.start_date = start_date
        self.end_date = end_date
        self.root_url = options['root_url']
        self.week_type = options['week_type']
        self.delta = 28 if self.week_type == '4WK' else 84

    def get_range(self):
        return_obj = []
        response = requests.get(self.root_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        for _, link in enumerate(soup.findAll('a')):
            if link.get('href').endswith('.tif.gz'):
                _, wk, rest = link.get('href').split('_')
                if wk == self.week_type:
                    end_time = datetime.datetime.strptime('{} {}'.format(rest[4:].replace('.tif.gz', ''), rest[:4]),
                                                      '%j %Y')
                    date = end_time - datetime.timedelta(days=self.delta)
                    filename = link.get_text()
                    if self.start_date <= date.date() <= self.end_date:
                        return_obj.append({
                            'date': date.strftime('%Y%m%d'),
                            'filename': filename,
                            'source': self.root_url + filename,
                        })

        return return_obj
