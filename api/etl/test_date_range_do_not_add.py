import datetime
import requests
from bs4 import BeautifulSoup

start_year = 2021
current_year = start_year
end_year = 2022

start_date = datetime.datetime(start_year, 11, 1)
end_date = datetime.datetime(end_year, 4, 30)

filenames = []
dates = []
# loop thru year directories
while current_year <= end_year:
    expected_file_name_wk_number_string = '12WK'
    response = requests.get("https://gis1.servirglobal.net/data/esi/" + expected_file_name_wk_number_string + "/" + str(current_year))
    soup = BeautifulSoup(response.text, 'html.parser')

    for _, link in enumerate(soup.findAll('a')):
        if link.get('href').endswith('.tif'):
            _, wk, rest = link.get('href').split('_')
            if wk == expected_file_name_wk_number_string:
                date = datetime.datetime.strptime('{} {}'.format(rest[4:].replace('.tif', ''), rest[:4]), '%j %Y')
                if start_date <= date <= end_date:
                    filenames.append(link.get_text())
                    dates.append(date)
    current_year = current_year + 1
