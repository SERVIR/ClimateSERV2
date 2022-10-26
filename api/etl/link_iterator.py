import datetime
import requests
import re

from bs4 import BeautifulSoup


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
        for _, link in enumerate(soup.findAll('a')):
            link_text = link.get('href')
            match = re.search(self.search, link_text)
            if match:
                date = datetime.datetime.strptime(match.group(0), '%Y%m%d')

                if self.indexedBy == 'start':
                    e_date = date + datetime.timedelta(days=self.timedelta)
                    if date.date() >= self.start_date and e_date.date() <= self.end_date:
                        dates.append(date)

        date_range_string = [date.strftime('%Y%m%d') for date in dates]

        return date_range_string
