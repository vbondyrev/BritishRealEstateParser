# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""This script scraping data from www.rightmove.co.uk"""

import os
import sys
import multiprocessing
from time import time, sleep
import csv
import re
import random
import sqlite3

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

RIGHTMOVE_URL = 'https://www.rightmove.co.uk/site-map.html'
FOL_PATH = os.path.join(os.path.expanduser('~'), 'app')
DB_PATH = os.path.join(os.path.expanduser('~'), 'app')
FOL_URL_PATH = os.path.join(os.path.expanduser('~'), 'app')

# list of User-Agents for security bypass (loop conf IP+UA)
LIST_UA = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
           'Mozilla/0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
           'Mozilla/5.0 (Windows NT 6.1; Win64; x86) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
           'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
           'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) \
             Gecko/20100101 Firefox/62.0'
           'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; \
             .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; \
             .NET4.0C; .NET4.0E; Media Center PC 6.0; CMNTDFJS; F9J; \
             InfoPath.3; rv:11.0) like Gecko',
           'Opera 12.17 (Win 8 x64): Opera/9.80 (Windows NT 6.2; WOW64) \
             Presto/2.12.388 Version/12.17',
           'Internet Explorer 11 (Win 8.1 x64): Mozilla/5.0 \
             (Windows NT 6.3; WOW64; Trident/7.0; ASU2JS; rv:11.0) like Gecko',
           'Mozilla/5.0 (Windows NT 6.1; WOW64; \
             Trident/7.0; AS; rv:11.0) like Gecko',
           'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
             AppleWebKit/537.36 (KHTML, like Gecko) \
             Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586']


# ==============================================================================
#                          --=== Step - 1 ===--
# ==============================================================================


def read_params():
    """
    Processing argv`s

    set by default requests interval 1 second
    """
    request_sec = 1
    try:
        if len(sys.argv) == 1:
            print(' Requests interval sets by default {:3.2f} sec' \
                  .format(request_sec), sep='\n')
        elif len(sys.argv) == 2 and int(sys.argv[1]) == 0:
            request_sec = 0
            print(' No Requests interval, in this case sets - 0 sec', sep='\n')
        elif len(sys.argv) == 2 and 0 < int(sys.argv[1]) <= 600:
            request_sec = 60 / float(sys.argv[1])
            print(' Requests interval sets manualy {:3.2f} sec' \
                  .format(request_sec), sep='\n')
        else:
            sys.exit(0)
    except AttributeError:
        print(' Wrong! Please, try by default - without parameters.')
        sys.exit(0)
    return request_sec


# ==============================================================================
#                          --=== Step - 2 ===--
# ==============================================================================


def pars_sitemap():
    """
    Reading site map and get listing A- B- C- links

    """
    abc_link = []
    # requests and check status_code if something wrong
    html = requests.get(RIGHTMOVE_URL,
                        headers={'User-Agent': str(random.choice(LIST_UA))},
                        stream=True)

    if html.status_code == 200:
        print(' Connection to {:s} is OK!'.format(html.url), sep='\n')
    else:
        print(' No connection. Status code: ' + str(html.status_code))

    soup = BeautifulSoup(html.text, features='html.parser')

    # Parsing sitemap
    for i in soup.find_all("a", {"class": None}):
        if re.search(r'/uk-property-search', str(i.get('href'))) is not None:
            abc_link.append(str(i.get('href')))

    # LIMIT FOR TESTING
    abc_link = abc_link[:10]

    # Savig result to CSV
    with open(FOL_URL_PATH + '\\1_links_SiteMap.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter='\n')
        writer.writerows([abc_link])

    print(' Parsing Site Map finished.' + '\n' +
          ' File "links_SiteMap.csv" with top links stored in {:s}'
          .format(FOL_URL_PATH))
    return len(abc_link)


# ==============================================================================
#                           --=== Step - 3 ===--
# ==============================================================================


def parse_by_geo(url=None):
    """
    Parse FULL listing all needed links 2nd level (by Geo position)

    """
    abc_link = []
    html = requests.get(url, headers=
                        {'User-Agent': str(random.choice(LIST_UA))})

    # Parsing step 1
    soup = BeautifulSoup(html.text, features='html.parser')

    # Parsing sitemap
    uls = soup.find('div', {'class': 'regionindex'})
    for ul_var in uls.find_all('ul'):
        for a_var in ul_var.find_all('a'):
            abc_link.append(str(a_var.get('href')))

    # Savig result to CSV
    if not os.path.isfile(FOL_URL_PATH + '\\2_links_ByGeo.csv'):
        with open(FOL_URL_PATH + '\\2_links_ByGeo.csv', 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter='\n')
            writer.writerows([abc_link])
    else:
        with open(FOL_URL_PATH + '\\2_links_ByGeo.csv', 'a') as csvfile:
            writer = csv.writer(csvfile, delimiter='\n')
            writer.writerows([abc_link])

    return abc_link


def write_db_prepare():
    """
    Prepare DB
    DROP/CREATE exists tables

    """
    d_var = {'main': 'objlink', 'type': 'typename',
             'price': 'price', 'agency': 'agencyname', 'images': 'imaglink'}
    db_var = sqlite3.connect(DB_PATH + '/localDB.db')
    my_cursor = db_var.cursor()
    for key, value in d_var.items():
        my_cursor.execute("DROP TABLE IF EXISTS {}".format(key))
    for key, value in db_var.items():
        my_cursor.execute('CREATE TABLE IF NOT EXISTS {}(id TEXT, {} TEXT)'
                          .format(key, value))
    my_cursor.close()
    db_var.commit()
    db_var.close()


def write_db(m_dict, typ_dict, prc_dict, agen_dict, im_dict):
    """
    INSERT into DB

    """
    db_var = sqlite3.connect(DB_PATH + '/localDB.db')
    my_cursor = db_var.cursor()

    my_cursor.execute('PRAGMA synchronous = OFF')
    my_cursor.execute('PRAGMA journal_mode = MEMORY')

    mdict_as_list = m_dict.items()
    for row in mdict_as_list:
        my_cursor.execute('INSERT INTO main VALUES (?, ?)', row)

    typdict_as_list = typ_dict.items()
    for row in typdict_as_list:
        my_cursor.execute('INSERT INTO type VALUES (?, ?)', row)

    prcdict_as_list = prc_dict.items()
    for row in prcdict_as_list:
        my_cursor.execute('INSERT INTO price VALUES (?, ?)', row)

    agendict_as_list = agen_dict.items()
    for row in agendict_as_list:
        my_cursor.execute('INSERT INTO agency VALUES (?, ?)', row)

    imdict_as_list = im_dict.items()
    for row in imdict_as_list:
        my_cursor.execute('INSERT INTO images VALUES (?, ?)', row)

    my_cursor.close()
    db_var.commit()
    db_var.close()


def get_html(link):
    """
    Get html with random User-Agent

    """
    response = requests.get(link,
                            headers={'User-Agent': str(random.choice(LIST_UA))},
                            stream=True)
    return response.text


def get_params(link=None):
    """
    Get all data from web and save to DB

    """
    html = get_html(link)
    soup = BeautifulSoup(html, features='html.parser')
    canonical_link = []
    canonical_id = []
    main_dict = dict()
    type_dict = dict()
    price_dict = dict()
    agency_dict = dict()
    img_dict = dict()
    sale_or_rent = []
    price = []
    agency_name = []
    id_obj = []
    img_obj = []

    # get Parametr 1
    divs = soup.find_all('div', class_='l-searchResult is-list ')
    for div in divs:
        if re.findall(r'property-to-rent', link):
            canonical_link.append(
                ('https://www.rightmove.co.uk/property-to-rent/'
                 + str(div.get('id'))
                 + str('.html')))
        elif re.findall(r'property-for-sale', link):
            canonical_link.append(
                ('https://www.rightmove.co.uk/property-for-sale/'
                 + str(div.get('id')) + str('.html')))
        else:
            canonical_link.append((str(div.get('id')) + str('.html')))
        canonical_id.append((''.join(re.findall(r'\b\d+\b',
                                                str(div.get('id'))))))

    main_dict = dict(zip(canonical_id, canonical_link))

    # get Parametr 2
    divs = soup.find_all('a', class_='propertyCard-headerLink')
    for item in divs:
        if re.search(r'/property-to-rent/', str(item.get('href'))) is not None:
            sale_or_rent.append('Rent')
        elif re.search(r'/property-for-sale/',
                       str(item.get('href'))) is not None:
            sale_or_rent.append('Sale')
        elif re.search(r'/commercial-property-to-let/',
                       str(item.get('href'))) is not None:
            sale_or_rent.append('Rent commercial')
        elif re.search(r'/commercial-property-for-sale/',
                       str(item.get('href'))) is not None:
            sale_or_rent.append('Sale commercial')
        else:
            sale_or_rent.append('Different type')

    type_dict = dict(zip(canonical_id, sale_or_rent))

    # get Parametr 3
    divs = soup.find_all('div', {'class': ['propertyCard-priceValue',
                                           'propertyCard-rentalPrice-primary']})
    for divn in enumerate(divs):
        price.append(str(divn.text).strip())

    price_dict = dict(zip(canonical_id, price))

    # get Parametr 4
    agency = soup.find_all('div', class_='propertyCard-contactsItem')
    for t_var in agency:
        for a_var in t_var.find_all('a', class_='propertyCard-branchLogo-link'):
            agency_name.append(a_var.get('title'))

    agency_dict = dict(zip(canonical_id, agency_name))

    # get Parametr 5
    alla = soup.find_all('a', class_='propertyCard-img-link aspect-3x2 ')
    for t_var in alla:
        id_obj.append(''.join(re.findall(r'\b\d+\b', str(t_var.get('href')))))
        if (t_var.get('href')) != "":
            for a_var in t_var.find_all('img', alt='Property Image 1'):
                img_obj.append(str(a_var.get('src')))

    img_dict = dict(zip(id_obj, img_obj))

    # Save result to DB
    write_db(main_dict, type_dict, price_dict, agency_dict, img_dict)


def write_db_csv():
    """
    Save data from DB to CSV

    """
    the_input = []

    # Dilog menu
    try:
        the_input = input('  - DO YOU WANT EXPORT ALL DATA TO  *.CSV FILE ? /'
                          '(If YES, press Y/y) ')
        if the_input in ['y', 'Y']:
            db_var = sqlite3.connect(DB_PATH + '/localDB.db')
            my_cursor = db_var.cursor()
            data = my_cursor.execute(''' SELECT m.id, m.objlink as canonicURL,/
                                t.typename,p.price, a.agencyname, i.imaglink
                                FROM  main as m
                                    LEFT JOIN type as t on m.id = t.id
                                    LEFT JOIN price as p on m.id = p.id
                                    LEFT JOIN agency as a on m.id = a.id
                                    LEFT JOIN images as i on m.id = i.id''')
            with open(FOL_PATH + '\\Data.csv', 'w') as csvfile:
                writer = csv.writer(csvfile, delimiter='\t')
                writer.writerow(['Id', 'CanonicURL', 'Type', 'Price', 'ID',
                                 'Agencyname', 'ImageLink'])
                writer.writerows(data)
                print("  - File with data saved in directory {:s}\\Data.csv."
                      .format(FOL_PATH), sep='\n')

            my_cursor.close()
            db_var.commit()
            db_var.close()
        else:
            print('\n')
    except AttributeError:
        print('\n')

def main():
    """
    main

    """
    cnt_links = 0
    links = []
    html_list = []

    # Create folders for contents
    if not os.path.exists(FOL_URL_PATH):
        print(' Path {:s} doesn''t exist. Directory for *.csv files'
              ' with URL created.'.format(FOL_URL_PATH), sep='\n')
        os.makedirs(FOL_URL_PATH)
    if not os.path.exists(DB_PATH):
        print(' Path {:s} doesn''t exist. Database directory '
              'created.'.format(DB_PATH), sep='\n')
        os.makedirs(DB_PATH)

    # Del previous csv files if exist
    if os.path.isfile(FOL_URL_PATH + '\\1_links_SiteMap.csv'):
        os.remove(FOL_URL_PATH + '\\1_links_SiteMap.csv')
    if os.path.isfile(FOL_URL_PATH + '\\2_links_ByGeo.csv'):
        os.remove(FOL_URL_PATH + '\\2_links_ByGeo.csv')

    print('\n' + '_____________________Step - 1____________________', sep='\n')
    request_sec = read_params()

    print('\n' + '_____________________Step - 2____________________', sep='\n')
    print(' Reading site map and get listing  hight level links', sep='\n')
    start = time()
    links_hight_level = pars_sitemap()
    print(' Parsing took: {:.2f} seconds'.format(time() - start), sep='\n')
    print(' Parsed {:d} links '.format(links_hight_level), sep='\n')

    print('\n' + '____________________Step - 3_____________________', sep='\n')
    print(' Reading hight level links and get listing second level:', sep='\n')

    if os.path.isfile(FOL_URL_PATH + '\\1_links_SiteMap.csv'):
        with open(FOL_URL_PATH + '\\1_links_SiteMap.csv', 'r',
                  encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file)
            html_list = [str(item) for sublist in list(reader)
                         for item in sublist]
            start = time()
            for i in tqdm(range(len(html_list)), total=len(html_list),
                          mininterval=0.5, desc='Parsing', unit='link'):
                abc_link = parse_by_geo(html_list[i])
                cnt_links = cnt_links + len(abc_link)
                links.extend(abc_link)
                sleep(request_sec)
            print('\n' + ' Parsing second level links finished.' + '\n' +
                  ' File "2_links_ByGeo.csv" stored in {:s}'
                  .format(FOL_URL_PATH))
            print(' Parsing took: {:.2f} seconds'
                  .format(time() - start), sep='\n')
            print(' Parsed {:d} links '.format(cnt_links), sep='\n')
    else:
        print(' No files ''1_links_SiteMap.csv'' with A, B, C '
              'links from site map', sep='\n')

    # Savig result to CSV
    if not os.path.isfile(FOL_URL_PATH + '\\2_links_ByGeo.csv'):
        with open(FOL_URL_PATH + '\\2_links_ByGeo.csv', 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter='\n')
            writer.writerows([links])
    else:
        with open(FOL_URL_PATH + '\\2_links_ByGeo.csv', 'a') as csvfile:
            writer = csv.writer(csvfile, delimiter='\n')
            writer.writerows([links])

    # TESTING LIMIT
    links = links[0:100]

    # Recreate DB
    print('\n' + '_____________________Step - 4____________________', sep='\n')
    write_db_prepare()

    # START multiprocessing POOLS
    start = time()
    print(' Processing, please waiting...', sep='\n')
    pool = multiprocessing.Pool(10)
    pool.map(get_params, links)
    pool.close()
    pool.join()
    print(' Parsing took: {:.2f} seconds'.format(time() - start), sep='\n')

    print('\n' + '_____________________Step - 5____________________', sep='\n')
    print(' Save results:')

    write_db_csv()
    print('\n' + ' ___Work done. Have a nice day! ________________|', sep='\n')


if __name__ == "__main__":
    main()
