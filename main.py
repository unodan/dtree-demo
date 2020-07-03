from csv import reader
from bs4 import BeautifulSoup
from pathlib import Path
from libs.dtree import DTree, Node, Leaf
from libs.maria import MariaDB
from zipfile import ZipFile
from urllib.request import urlretrieve, urlopen
from enum import IntEnum

import time
import logging as lg
import pandas as pd
import requests

_path = Path(__file__).cwd()


class App:
    def __init__(self, user_profile, **kwargs):
        super().__init__()
        self.user_profile = user_profile

        for _dir in ('csv', 'src', 'logs'):
            _path.joinpath(_dir).mkdir(exist_ok=True)

        if not _path.joinpath('csv', 'countries.csv').exists():
            self.get_country_csv_file()

        files = _path.joinpath('csv').glob('*SubdivisionCodes.csv')
        if not any(True for _ in files):
            self.get_country_zone_csv_files()

        files = _path.joinpath('csv').glob('*_all.csv')
        if not any(True for _ in files):
            self.get_country_place_info_csv_file()

        filename = _path.joinpath('logs', kwargs.get('filename', 'maria.log'))
        database = user_profile['database']
        db = self.db = MariaDB(log_file=str(filename), log_level=kwargs.get('log_level', lg.ERROR))
        self.connected = True if db.connect(database, connection=user_profile) else False

        self.start()

    @staticmethod
    def get_unlocode_files():
        file_list = []

        for file_name in _path.joinpath('csv').iterdir():
            name = str(file_name.resolve())
            if 'UNLOCODE' in name and name.endswith('.csv'):
                file_list.append(file_name)
        return tuple(sorted(file_list))

    @staticmethod
    def get_country_csv_file():
        print('\nDownloading csv files to populate database with.\n')
        url = 'https://www.iban.com/country-codes'
        with urlopen(url) as f:
            html = f.read()
            soup = BeautifulSoup(html, features="html.parser")
            li = soup.find("table", {"id": "myTable"})
            table_body = li.find('tbody')
            rows = table_body.findChildren("tr")
            print('Downloading:', url)
            filename = _path.joinpath('csv', 'countries.csv')
            with open(str(filename), "w") as text_file:
                for row in rows:
                    line = ''
                    for idx, column in enumerate(row.text.split('\n')[:4]):
                        if ',' in column:
                            col = column.split(',')
                            column = f'{col[0].strip(" ")} ({col[1].strip(" ")})'
                        line += f'{column},'
                    print(line.strip(','), file=text_file)

    @staticmethod
    def get_country_zone_csv_files():
        with urlopen('http://www.unece.org/cefact/codesfortrade/codes_index.html') as response:
            html = response.read()

        soup = BeautifulSoup(html, features="html.parser")
        parsed_data = soup.find("div", {"id": "c21211"})
        version_number = parsed_data.find_all(['td'])[3].text.split()[-1:][0]
        unlocode_zip_file = 'loc' + version_number.replace('-', '')[2:] + 'csv.zip'

        url = 'http://www.unece.org/fileadmin/DAM/cefact/locode/' + unlocode_zip_file
        src_file_name = url.split('/')[-1]
        filename = _path.joinpath('src', src_file_name)

        if not filename.exists():
            url = 'http://www.unece.org/fileadmin/DAM/cefact/locode/' + unlocode_zip_file
            print('Downloading:', url)
            urlretrieve(url, filename=str(filename))

            with ZipFile(str(filename), 'r') as z:
                z.extractall('./csv')

    @staticmethod
    def get_country_place_info_csv_file():
        url = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/cities/totals'
        with urlopen(url) as response:
            html = response.read()
            soup = BeautifulSoup(html, features="html.parser")
            links = soup.find('table').findAll('a')

            filename = _path.joinpath('csv', links[-1].text)
            if not filename.exists():
                url = f'{url}/{links[-1].text}'
                print('Downloading:', url)
                urlretrieve(url, filename=str(filename))

    def start(self):
        def tables():
            return (
                ('country',
                 'id INT UNSIGNED NOT NULL AUTO_INCREMENT, '
                 'name VARCHAR(100) UNIQUE NOT NULL, '
                 'code2 VARCHAR(2) UNIQUE NOT NULL, '
                 'code3 VARCHAR(3) UNIQUE NOT NULL, '
                 'population INT UNSIGNED, '
                 'PRIMARY KEY (id)'
                 ),
                ('country_zone',
                 'id INT UNSIGNED NOT NULL AUTO_INCREMENT, '
                 'country_id INT UNSIGNED NOT NULL, '
                 'code VARCHAR(3) NOT NULL, '
                 'name VARCHAR(100) NOT NULL, '
                 'type VARCHAR(60), '
                 'population INT UNSIGNED, '
                 'PRIMARY KEY (id), '
                 'INDEX (country_id), '
                 'CONSTRAINT id_code UNIQUE (country_id, code), '
                 'FOREIGN KEY (country_id) '
                 'REFERENCES country (id) '
                 'ON DELETE CASCADE',
                 ),
                ('country_place',
                 'id INT UNSIGNED NOT NULL AUTO_INCREMENT, '
                 'zone_id INT UNSIGNED NOT NULL, '
                 'code VARCHAR(3) NOT NULL, '
                 'name VARCHAR(256) NOT NULL, '
                 'type VARCHAR(20), '
                 'population INT UNSIGNED, '
                 'flags VARCHAR(9), '
                 'coordinates VARCHAR(16), '
                 'PRIMARY KEY (id), '
                 'INDEX (zone_id), '
                 'CONSTRAINT id_code UNIQUE (zone_id, code), '
                 'FOREIGN KEY (zone_id) '
                 'REFERENCES country_zone (id) '
                 'ON DELETE CASCADE',
                 ),
            )

        def init_tables():
            db = self.db
            for table in tables():
                table_name, table_sql = table
                if not db.table_exist(table_name):
                    db.create_table(table_name, table_sql)

            if db.execute('SELECT COUNT(*) FROM country'):
                if not db.fetchone()[0]:
                    print('\nUpdating table country.')
                    self.update_country()

            if db.execute('SELECT COUNT(*) FROM country_zone'):
                if not db.fetchone()[0]:
                    print('Updating table country_zone.')
                    self.update_country_zone()

            if db.execute('SELECT COUNT(*) FROM country_place'):
                if not db.fetchone()[0]:
                    print('Updating table country_place, this table takes about 15 minutes to update.')
                    self.update_country_place()
                    self.update_country_place_info()
                    print('Update of database successful.')

        init_tables()

    def drop_db_tables(self):
        if self.connected:
            for table in ('country_place', 'country_zone', 'country'):
                if self.db.execute('SHOW TABLES LIKE %s;', (table,)):
                    self.db.execute(f'DROP TABLE {table};')

    def update_country(self):
        filename = _path.joinpath('csv', 'countries.csv')
        if not filename.exists():
            self.get_country_csv_file()

        if filename.exists():
            country = IntEnum('Country', 'name code2 code3 population', start=0)
            with open(str(filename.resolve())) as f:
                results = reader(f, delimiter=',', quotechar='"')

                for row in results:
                    sql = 'SELECT id FROM country WHERE country.code2="%s";'
                    if self.db.execute(sql, (row[1].strip(' '),)):
                        continue

                    self.db.insert_row('country', (
                        row[int(country.name)].strip(' '),
                        row[int(country.code2)].strip(' '),
                        row[int(country.code3)].strip(' '),
                        None,
                    ))

    def update_country_zone(self):
        reject = [
            'parish', 'dependency', 'department', 'federal district', 'autonomous district', 'island council',
            'autonomous region', 'special administrative region', 'special municipality', 'administration',
            'metropolitan department', 'council area', 'district council area', 'local council',
            'administrative atoll', 'zone', 'autonomous city', 'administrative region', 'administrative territory',
            'oblast', 'economic prefecture', 'department', 'departments', 'free communal consortia', 'town council',
        ]

        zone = IntEnum('Zone', 'country_code2 code name type population', start=0)
        files = list(_path.joinpath('csv').glob('*SubdivisionCodes.csv'))
        if files:
            filename = str(files[0].resolve())
            with open(filename, errors='ignore') as f:
                results = reader(f, delimiter=',', quotechar='"')

                for idx, row in enumerate(results):
                    if row[3].lower() in reject:
                        continue

                    for column, value in enumerate(row):
                        j = value.replace('?', '').replace('\n', ' ')
                        row[column] = j

                    sql = 'SELECT id FROM country WHERE country.code2=%s;'
                    if self.db.execute(sql, (row[0],)):
                        _id = self.db.fetchone()[0]
                        self.db.insert_row('country_zone', (
                            _id,
                            row[int(zone.code)],
                            row[int(zone.name)],
                            row[int(zone.type)],
                            None,
                        ))

    def update_country_place(self):
        for file in self.get_unlocode_files():
            file = _path.joinpath('src', file).resolve()
            with open(str(file), errors='ignore') as f:
                results = reader(f, delimiter=',', quotechar='"')
                for i, row in enumerate(results):

                    place = IntEnum(
                        'Place', '_changed country_code2 code _name name zone_code flags _2 _3 _4 coordinates',
                        start=0)

                    sql = 'SELECT id FROM country WHERE country.code2=%s;'
                    if self.db.execute(sql, (row[int(place.country_code2)],)):
                        country_id = self.db.fetchone()[0]
                        sql = 'SELECT country_zone.id ' \
                              'FROM country_zone ' \
                              'WHERE country_zone.country_id=%s AND country_zone.code=%s;'
                        if self.db.execute(sql, (country_id, row[int(place.zone_code)])):
                            results = self.db.fetchone()
                            zone_id = results[0] if results else None
                            if not zone_id:
                                continue

                            self.db.insert_row('country_place', (
                                zone_id,
                                row[int(place.code)],
                                row[int(place.name)],
                                None,
                                None,
                                row[int(place.flags)],
                                row[int(place.coordinates)],
                            ))

    def update_country_place_info(self):
        files = sorted(list(_path.joinpath('csv').glob('*_all.csv')), reverse=True)
        if files:
            filename = str(files[0])
            with open(filename, errors='ignore') as f:
                results = reader(f, delimiter=',', quotechar='"')
                next(results)

                filename = str(_path.joinpath('csv', filename.replace('_all', '')))
                with open(filename, 'w') as file_object:
                    lines = set()

                    for row in results:
                        if row[8].endswith('County'):
                            continue
                        _end = row[8].rsplit(' ', 1).pop()

                        result = row[8] if _end in ('city', 'town', 'village') else None
                        if not result or 'Balance of' in row[8]:
                            continue
                        _place, _type = row[8].rsplit(' ', 1)
                        _pop = row[-1]
                        lines.add(f'{_type.capitalize()},{_pop},{row[9]},{_place}')

                    for line in lines:
                        file_object.write(f'{line}\n')

                with open(filename, errors='ignore') as f:
                    results = reader(f, delimiter=',', quotechar='"')
                    for i, row in enumerate(results):
                        sql = 'UPDATE country_place  ' \
                              'JOIN country_zone  ' \
                              'ON country_zone.id = country_place.zone_id  ' \
                              'JOIN country  ' \
                              'ON country.id = country_zone.country_id  ' \
                              'SET country_place.type=%s, country_place.population=%s  ' \
                              'WHERE country_zone.name=%s AND country_place.name=%s;'

                        result = self.db.execute(sql, row)
                        print((i, result, row))


def main():
    start = time.time()

    app = App({
        'host': 'localhost',
        'port': 3306,
        'user': 'mary',
        'password': 'password',
        'database': 'countries',
    })

    def us_census():
        url = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/cities/totals'
        with urlopen(url) as response:
            html = response.read()
            soup = BeautifulSoup(html, features="html.parser")
            links = soup.find('table').findAll('a')

            filename = _path.joinpath('csv', links[-1].text)
            if not filename.exists():
                url = f'{url}/{links[-1].text}'
                print('Downloading:', url)
                urlretrieve(url, filename=str(filename))

    def ca_census2():
        url = 'https://www12.statcan.gc.ca/census-recensement/2016/dp-pd/prof/details/' \
              'download-telecharger/comp/page_dl-tc.cfm?Lang=E'

        with urlopen(url) as response:
            html = response.read()
            soup = BeautifulSoup(html, features="html.parser")
            table = soup.find("table", {"id": "dataset-filter"})
            rows = table.find('tbody').find_all('tr')

            filename = _path.joinpath('csv', 'census_canada')
            # if not filename.exists():
            filename.mkdir(parents=True, exist_ok=True)

            for i, r in enumerate(rows):
                dir_name = f'{r.find("th").text}'
                filename = _path.joinpath('csv', 'census_canada', dir_name)
                if not filename.exists():
                    filename.mkdir(parents=True, exist_ok=True)

                dst_file = filename.joinpath('census.zip')
                if not dst_file.exists():
                    print(i, f'https://www12.statcan.gc.ca{r.find("a")["href"]}')
                    req = requests.get(f'https://www12.statcan.gc.ca{r.find("a")["href"]}')
                    url_content = req.content
                    csv_file = open(str(filename.joinpath('census.zip')), 'wb')
                    csv_file.write(url_content)
                    csv_file.close()

    def ca_census3():
        url = 'https://www12.statcan.gc.ca/census-recensement/2016/dp-pd/prof/details/download-telecharger/comp/' \
              'GetFile.cfm?Lang=E&FILETYPE=CSV&GEONO=048'

        filename = _path.joinpath('src', 'canada_census.zip')
        if not filename.exists():
            print('Downloading:', url)
            urlretrieve(url, filename=str(filename))

        with ZipFile(str(filename), 'r') as zipObj:
            # Get a list of all archived file names from the zip
            listOfFileNames = zipObj.namelist()
            # Iterate over the file names
            for fileName in listOfFileNames:
                # Check filename endswith csv
                if fileName.endswith('.csv'):
                    # Extract a single file from zip
                    zipObj.extract(fileName, _path.joinpath('csv'))

    def ca_census():
        url = 'https://www12.statcan.gc.ca/census-recensement/2016/dp-pd/prof/details/' \
              'download-telecharger/comp/page_dl-tc.cfm?Lang=E'

        with urlopen(url) as response:
            html = response.read()
            soup = BeautifulSoup(html, features="html.parser")
            table = soup.find("table", {"id": "dataset-filter"})
            rows = table.find('tbody').find_all('tr')

            for i, r in enumerate(rows):
                name = f'{r.find("th").text}'
                filename = _path.joinpath('csv', 'census_canada', name)
                if not filename.exists():
                    filename.mkdir(parents=True, exist_ok=True)

        file = _path.joinpath('csv', 'census_canada',
                              'Census subdivisions (CSD) - Ontario only', 'Geo_starting_row_CSV.csv')
        if file.exists():
            df = pd.read_csv(str(file))

            for index, row in df.iterrows():
                print(row.values)

    # Create a tree to populate.
    tree = DTree(unique=False)

    regx = r'\([^)]*\)'  # Strip out data and brackets if found in the name.

    # app.update_country_place_info()
    # ca_census()

    # sql = 'SELECT id, name, code2, code3 FROM country WHERE code2="US" OR code2="CA" ORDER BY name;'
    # if app.db.execute(sql):
    #     for country_row in app.db.fetchall():
    #         country_id, country_name, code2, code3 = country_row
    #         country_name = re.sub(regx, '', country_name).strip()
    #         country_tree = DTree(
    #             parent=tree, name=country_name, errors='ignore')
    #         country_tree.populate([
    #             {'name': 'type', 'columns': ['Country']},
    #             {'name': 'code', 'columns': [code2]},
    #             {'name': 'code3', 'columns': [code3]},
    #             {'name': 'zones', 'children': []},
    #         ])
    #
    #         sql = 'SELECT id, name, code, type, population FROM country_zone WHERE country_id=%s ORDER BY name;'
    #         if app.db.execute(sql, (country_id, )):
    #             for zone_row in app.db.fetchall():
    #                 zone_id, zone_name, code, _type, population = zone_row
    #                 zone_tree = DTree(
    #                     parent=country_tree.query('zones'), name=zone_name, errors='ignore')
    #                 zone_tree.populate([
    #                     {'name': 'type', 'columns': [_type]},
    #                     {'name': 'population', 'columns': [population]},
    #                     {'name': 'code', 'columns': [code]},
    #                     {'name': 'places', 'children': []},
    #                 ])
    #
    #                 sql = 'SELECT name, code, type, population FROM country_place WHERE zone_id=%s ORDER BY name;'
    #                 if app.db.execute(sql, (zone_id, )):
    #                     for place_row in app.db.fetchall():
    #                         place_name, code, _type, population = place_row
    #                         place_tree = DTree(
    #                             parent=zone_tree.query('places'), name=place_name)
    #                         place_tree.populate([
    #                             {'name': 'code', 'columns': [code]},
    #                             {'name': 'type', 'columns': [_type]},
    #                             {'name': 'population', 'columns': [population]},
    #                         ])
    #
    #     # on = tree.query('Hamilton')
    #     # on.show(show_columns=True)
    #     #
    #     # tree.show()
    #     #
    #     # ca = tree.query('Youngstown')
    #     # ca.show(show_columns=True)
    #     #
    #     #
    #     # tree.query('Ontario/places/Hamilton').show(show_columns=True)
    #     #
    #     # us = tree.query('United States of America')
    #
    #     # pl = tree.find_all('Hamilton', recursive=True)
    #     # for i in pl:
    #     #     print(f'"{i.parent.parent.name}", "{i.name}"')
    #
    #     print()
    #
    #     # Displays all the places in the state of New York in the USA.
    #     # ny_places = tree.query('United States of America/New York/places').show()
    #     # print(len(ny_places))
    #     #
    #     # Displays all the places in the state of New York in the USA.
    #     # sql = f'SELECT country.name, country_zone.name, country_zone.code, country_place.name ' \
    #     #       f'FROM country ' \
    #     #       f'JOIN country_zone ' \
    #     #       f'ON country.id=country_zone.country_id ' \
    #     #       f'JOIN country_place ' \
    #     #       f'ON country_zone.id=country_place.zone_id ' \
    #     #       f'WHERE country.code2="US" AND country_zone.code="NY" ' \
    #     #       f'ORDER BY country_place.name;'
    #     # if app.db.execute(sql):
    #     #     for idx, row in enumerate(app.db.fetchall(), start=1):
    #     #         print(idx, row)
    #
    #     print()
    #
    #     # ham1 = tree.query('Canada/Hamilton')
    #
    #     # ham2 = Node(name='copy')
    #
    #     # ham1.clone(ham2)
    #
    #     # ham1.show()
    #     # ham2.show()
    #
    #     # print(ham1.query('code').get(1), ham2.query('code').get(1))
    #
    #     # print(id(ham1), id(ham2))
    #
    #     # print(ham.type, ham.query('code').get(1))
    #
    #     # tree.show()
    #     # print(len(tree))
    #
    #     # ca = tree.query('United States of America/New York')
    #     # ca.show()

    # df0 = pd.read_csv(str(_path.joinpath('csv', 'Geo_starting_row_CSV.csv')), low_memory=False)
    # df = pd.read_csv(str(_path.joinpath('csv', '98-401-X2016048_English_CSV_data.csv')), low_memory=False)

    fields = [
        'GEO_NAME',
        'Dim: Sex (3): Member ID: [1]: Total - Sex',
        'Dim: Sex (3): Member ID: [2]: Male',
        'Dim: Sex (3): Member ID: [3]: Female',
    ]

    # xx = df[fields]
    #
    # for c in xx.iteritems():
    #     print(c)
    #     break

    # print(df.columns)
    # print(df['Dim: Sex (3): Member ID: [1]: Total - Sex'])

    # print(df0[['Geo Name', 'Line Number']])
    # print(df0[['Geo Name', 'Line Number']].iloc[1:3])
    # on = tree.query('Canada/Ontario')
    # on.show(show_id=True)
    ca_census2()

    end = time.time()
    print(f'\nRuntime: '
          f'{str(int(((end - start) / 3660))).zfill(2)}:'
          f'{str(int((end - start) / 60)).zfill(2)}:'
          f'{str(int(((end - start) % 60))).zfill(2)}\n')


if __name__ == '__main__':
    main()
