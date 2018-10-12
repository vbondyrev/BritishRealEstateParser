# !/usr/bin/env python -i
# -*- coding: utf-8 -*-
import os
import sys
from multiprocessing import Pool
import multiprocessing
from pathlib import Path
from time import time, sleep
import csv
import re
import random
import requests
from bs4 import BeautifulSoup
import sqlite3
from tqdm import tqdm
from contextlib import closing


RIGHTMOVE_URL = 'https://www.rightmove.co.uk/site-map.html'
FOL_PATH = os.path.join(os.path.expanduser('~'), 'app')
DB_PATH = os.path.join(os.path.expanduser('~'), 'app')
FOL_URL_PATH = os.path.join(os.path.expanduser('~'), 'app')

# list of User-Agents for security bypass (loop conf IP+UA)
LIST_UA = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
             'Mozilla/0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
             'Mozilla/5.0 (Windows NT 6.1; Win64; x86) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
             'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
             'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0'
             'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E; Media Center PC 6.0; CMNTDFJS; F9J; InfoPath.3; rv:11.0) like Gecko',
             'Opera 12.17 (Win 8 x64): Opera/9.80 (Windows NT 6.2; WOW64) Presto/2.12.388 Version/12.17',
             'Internet Explorer 11 (Win 8.1 x64): Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; ASU2JS; rv:11.0) like Gecko',
             'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586']

# ===============================================================================
#                          --=== Step - 1 ===--
# Processing argv`s
# ===============================================================================
def read_params():
# set by default requests interval 1 second
    argReqSec = 1
    try:
        if len(sys.argv) == 1:
            print(' Requests interval sets by default {:3.2f} sec'.format(argReqSec), sep='\n')
        elif len(sys.argv) == 2 and int(sys.argv[1]) == 0:
            argReqSec = 0   # without TIMEOUT
            print(' No Requests interval, in this case sets - 0 sec'.format(argReqSec), sep='\n')
        elif len(sys.argv) == 2 and 0 < int(sys.argv[1]) <= 600:
            argReqSec = 60/float(sys.argv[1])
            print(' Requests interval sets manualy {:3.2f} sec'.format(argReqSec), sep='\n')
        else:
            sys.exit(0)
    except:
        	print(' Wrong! Please, try by default - without parameters.')
        	sys.exit(0)
    return argReqSec

# ===============================================================================
#                          --=== Step - 2 ===--
# Reading site map and get listing A- B- C- links
# ===============================================================================
def pars_SiteMap():
    abc_link = []

# requests and check status_code if something wrong
    html = requests.get(RIGHTMOVE_URL, \
        headers = {'User-Agent': str(random.choice(LIST_UA))}, stream = True)

    print(' Connection to {:s} is OK!'.format(html.url), sep='\n') \
            if html.status_code == 200 \
            else print(' No connection. Status code: ' + str(html.status_code))

    soup = BeautifulSoup(html.text, features='html.parser')

# Parsing sitemap
    for i in soup.find_all("a", {"class": None}):
        if re.search(r'/uk-property-search', str(i.get('href'))) is not None:
            abc_link.append(str(i.get('href')))

# TESTING
    abc_link = abc_link[:10]

# Savig result to CSV
    with open(FOL_URL_PATH + '\\1_links_SiteMap.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter='\n')
        writer.writerows([abc_link])

    print(' Parsing Site Map finished.' + '\n' + \
          ' File "links_SiteMap.csv" with top links stored in {:s}'.format(FOL_URL_PATH))
    return len(abc_link)

# ===============================================================================
#                           --=== Step - 3 ===--
#  Parse FULL listing all needed links 2nd level (by Geo position)
# ===============================================================================
def parse_ByGeo(url=None):

    abc_link =[]
    html = requests.get(url, headers = {'User-Agent': str(random.choice(LIST_UA))})

# Parsing step 1
    soup = BeautifulSoup(html.text, features='html.parser')

# Parsing sitemap
    uls = soup.find('div', {'class':'regionindex'})
    for ul in uls.find_all('ul'):
        for a in ul.find_all('a'):
            abc_link.append(str(a.get('href')))

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

def writeDB_prepare():
    d =  {'main':'objlink', 'type':'typename', 'price':'price', 'agency':'agencyname', 'images':'imaglink'}

    db=sqlite3.connect(DB_PATH + '/localDB.db')
    c = db.cursor()

    for key, value in d.items():
        c.execute("DROP TABLE IF EXISTS {}".format(key))

    for key, value in d.items():
        c.execute("CREATE TABLE IF NOT EXISTS {}(id TEXT, {} TEXT)".format(key, value))

    c.close()
    db.commit()
    db.close()

def writeDB(mDict, typDict, prcDict, agenDict, imDict):
    db=sqlite3.connect(DB_PATH + '/localDB.db')
    c = db.cursor()

    c.execute("PRAGMA synchronous = OFF")
    c.execute("PRAGMA journal_mode = MEMORY")

    mDict_as_list = mDict.items()
    for row in mDict_as_list:
        c.execute("INSERT INTO main VALUES (?, ?)", row)

    typDict_as_list = typDict.items()
    for row in typDict_as_list:
        c.execute("INSERT INTO type VALUES (?, ?)", row)

    prcDict_as_list = prcDict.items()
    for row in prcDict_as_list:
        c.execute("INSERT INTO price VALUES (?, ?)", row)

    agenDict_as_list = agenDict.items()
    for row in agenDict_as_list:
        c.execute("INSERT INTO agency VALUES (?, ?)", row)

    imDict_as_list = imDict.items()
    for row in imDict_as_list:
        c.execute("INSERT INTO images VALUES (?, ?)", row)

    c.close()
    db.commit()
    db.close()

def get_html(link):
    response = requests.get(link, headers = {'User-Agent': str(random.choice(LIST_UA))}, stream = True)
    return response.text

def get_params(link=None):
    html = get_html(link)
    soup = BeautifulSoup(html, features='html.parser')
    canonical_link = []
    canonical_id = []

    mainDict = dict()
    typeDict = dict()
    priceDict = dict()
    agencyDict = dict()
    imgDict = dict()
    sale_OrRent=[]
    price = []
    agency_Name = []
    id_Obj = []
    img_Obj = []

# get Parametr 1
    divs = soup.find_all('div', class_='l-searchResult is-list ')
    for div in divs:
        if re.findall(r'property-to-rent', link):
           canonical_link.append(('https://www.rightmove.co.uk/property-to-rent/' + str(div.get('id')) + str('.html')))
        elif re.findall(r'property-for-sale', link):
            canonical_link.append(('https://www.rightmove.co.uk/property-for-sale/' + str(div.get('id')) + str('.html')))
        else:
            canonical_link.append((str(div.get('id')) + str('.html')))
        canonical_id.append((''.join(re.findall(r'\b\d+\b', str(div.get('id'))))))
    mainDict = dict(zip(canonical_id, canonical_link))

# get Parametr 2
    divs = soup.find_all('a', class_='propertyCard-headerLink')
    for item in divs:
        if re.search(r'/property-to-rent/', str(item.get('href'))) is not None:
            sale_OrRent.append('Rent')
        elif re.search(r'/property-for-sale/', str(item.get('href'))) is not None:
            sale_OrRent.append('Sale')
        elif re.search(r'/commercial-property-to-let/', str(item.get('href'))) is not None:
            sale_OrRent.append('Rent commercial')
        elif re.search(r'/commercial-property-for-sale/', str(item.get('href'))) is not None:
            sale_OrRent.append('Sale commercial')
        else: sale_OrRent.append('Different type')
    typeDict = dict(zip(canonical_id, sale_OrRent))

# get Parametr 3
    divs = soup.find_all('div', {'class':['propertyCard-priceValue', 'propertyCard-rentalPrice-primary']})
    for i, div in enumerate(divs):
        price.append(str(div.text).strip())
    priceDict = dict(zip(canonical_id, price))

# get Parametr 4
    agency = soup.find_all('div', class_='propertyCard-contactsItem')
    for t in agency:
        for a in t.find_all('a', class_='propertyCard-branchLogo-link'):
            agency_Name.append(a.get('title'))
    agencyDict = dict(zip(canonical_id, agency_Name))

# get Parametr 5
    alla = soup.find_all('a', class_='propertyCard-img-link aspect-3x2 ')
    for t in alla:
        id_Obj.append(''.join(re.findall(r'\b\d+\b', str(t.get('href')))))
        if (t.get('href')) != "":
            for a in t.find_all('img', alt='Property Image 1'):
                img_Obj.append(str(a.get('src')))
    imgDict = dict(zip(id_Obj,img_Obj))

#Save result to DB
    writeDB(mainDict, typeDict, priceDict, agencyDict, imgDict)

def write_DB_CSV():

    the_input = []
# Dilog menu
    try:
        the_input = input("  - DO YOU WANT EXPORT ALL DATA TO  *.CSV FILE ? (If YES, press Y/y) ")
        if the_input == 'y' or the_input == 'Y':
            db=sqlite3.connect(DB_PATH + '/localDB.db')
            c = db.cursor()
            data = c.execute(''' SELECT m.id, m.objlink as canonicURL,t.typename,p.price, a.agencyname, i.imaglink
                                 FROM  main as m
                                    LEFT JOIN type as t on m.id = t.id
                                    LEFT JOIN price as p on m.id = p.id
                                    LEFT JOIN agency as a on m.id = a.id
                                    LEFT JOIN images as i on m.id = i.id''')
            with open(FOL_PATH + '\\Data.csv', 'w') as csvfile:
                writer = csv.writer(csvfile, delimiter='\t')
                writer.writerow(['Id', 'CanonicURL', 'Type', 'Price', 'ID', 'Agencyname', 'ImageLink'])
                writer.writerows(data)
                print("  - File with data saved in directory {:s}\\Data.csv.".format(FOL_PATH), sep='\n')

            c.close()
            db.commit()
            db.close()
        else:
            print('\n')
    except:
        print('\n')


def main():

    cnt_Links = 0
    links = []
    html_list = []

# Create folders for contents
    if not os.path.exists(FOL_URL_PATH):
        print(" Path {:s} doesn't exist. Directory for *.csv files with URL created.".format(FOL_URL_PATH), sep='\n')
        os.makedirs(FOL_URL_PATH)
    if not os.path.exists(DB_PATH):
         print(" Path {:s} doesn't exist. Database directory created.".format(DB_PATH), sep='\n')
         os.makedirs(DB_PATH)

# Del previous csv files if exist
    if os.path.isfile(FOL_URL_PATH + '\\1_links_SiteMap.csv'):
        os.remove(FOL_URL_PATH + '\\1_links_SiteMap.csv')
    if os.path.isfile(FOL_URL_PATH + '\\2_links_ByGeo.csv'):
        os.remove(FOL_URL_PATH + '\\2_links_ByGeo.csv')

    print('\n' + '_____________________Step - 1_____________________', sep='\n')
    argReqSec = read_params()

    print('\n' + '_____________________Step - 2_____________________', sep='\n')
    print(' Reading site map and get listing  hight level links', sep='\n')
    start = time()
    linksHiLevel = pars_SiteMap()
    print(' Parsing took: {:.2f} seconds'.format(time() - start), sep='\n')
    print(' Parsed {:d} links '.format(linksHiLevel), sep='\n')

    print('\n' + '_____________________Step - 3_____________________', sep='\n')
    print(' Reading  hight level links and get listing second level:', sep='\n')

    if os.path.isfile(FOL_URL_PATH + '\\1_links_SiteMap.csv'):
        with open(FOL_URL_PATH + '\\1_links_SiteMap.csv', 'r', encoding='utf-8') as csvFile:
            reader = csv.reader(csvFile)
            html_list = [str(item) for sublist in list(reader) for item in sublist]
            start = time()
            for i in tqdm(range(len(html_list)), total=len(html_list), \
                          mininterval=0.5, esc='Parsing', unit='link'):
                abc_link = parse_ByGeo(html_list[i])  # parse_ByGeo()
                cnt_Links = cnt_Links + len(abc_link)
                links.extend(abc_link)
                sleep(argReqSec)
            print('\n' + ' Parsing second level links finished.' + '\n' + \
                  ' File "2_links_ByGeo.csv" stored in {:s}'.format(FOL_URL_PATH))
            print(' Parsing took: {:.2f} seconds'.format(time() - start), sep='\n')
            print(' Parsed {:d} links '.format(cnt_Links), sep='\n')
    else:
       print(' No files ''1_links_SiteMap.csv'' with A, B, C links from site map', sep='\n')

# Savig result to CSV
    if not os.path.isfile(FOL_URL_PATH + '\\2_links_ByGeo.csv'):
        with open(FOL_URL_PATH + '\\2_links_ByGeo.csv', 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter='\n')
            writer.writerows([links])
    else:
        with open(FOL_URL_PATH + '\\2_links_ByGeo.csv', 'a') as csvfile:
            writer = csv.writer(csvfile, delimiter='\n')
            writer.writerows([links])

# TESTING
    links = links[0:100]

    print('\n' + '_____________________Step - 4_____________________', sep='\n')
# Recreate DB
    writeDB_prepare()

# START multiprocessing POOLS
    start = time()
    print(' Processing, please waiting...', sep='\n')
    pool = multiprocessing.Pool(10)
    pool.map(get_params, links)
    pool.close()
    pool.join()
    print(' Parsing took: {:.2f} seconds'.format(time() - start), sep='\n')

    print('\n' + '_____________________Step - 5_____________________', sep='\n')
    print(' Save results:')
    write_DB_CSV()
    print('\n' + ' ___Work done. Have a nice day! _________________|', sep='\n')

if __name__ == "__main__":
    main()