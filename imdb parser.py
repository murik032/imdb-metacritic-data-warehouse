import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import numpy as np
import multiprocessing
import logging
import csv
import psycopg2
from functools import partial
import concurrent.futures

def get_full_cast_from_link(url):
    """main data collector for single url"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    resulting_row = []
    response = requests.get(url, headers=headers)
    #print(response.status_code)
    soup = BeautifulSoup(response.content, 'html.parser')
    resulting_row.append(url)
    
    #1st - name
    m_name = soup.find(class_='hero__primary-text')
    resulting_row.append(m_name.text if m_name else None)
    
    #2nd - original name
    temp = soup.find(class_='sc-d8941411-1')
    if temp:
        original_name_list = temp.text.split(' ')
        resulting_row.append(' '.join(original_name_list[2:]))
    else:
        resulting_row.append(None)
   
    #3rd - year and certificate
    temp = soup.find_all('a', {
        'class': 'ipc-link ipc-link--baseAlt ipc-link--inherit-color',
        'role': 'button' 
    })
    list_of_tags = [i.text for i in temp]
    year = None
    cert = None
    if list_of_tags:
        for tag in list_of_tags:
            if tag.isdigit() and len(tag) == 4:
                year = int(tag)
        if list_of_tags[-1] != year:
            cert = list_of_tags[-1]
        resulting_row.append(year)
        resulting_row.append(cert)
    else:
        resulting_row.append(year)
        resulting_row.append(cert)
    
    #4th - rating (in stars)
    temp = soup.find(class_='sc-eb51e184-1')
    if temp:
        if temp.text:
            try:
                resulting_row.append(float(temp.text))
            except ValueError:
                resulting_row.append(None)
        else:
            resulting_row.append(None)
    else:
        resulting_row.append(None)
    #5th - genre
    genre = [j.text for j in soup.find_all(class_='ipc-chip__text') if j.text]
    resulting_row.append(genre[:-1])

    #6th - budget and worldwide gross
    box_office = soup.find('section', {'data-testid': 'BoxOffice'})
    if box_office:
        budget_label = box_office.find('span', class_='ipc-metadata-list-item__label', string='Budget')
        if budget_label:
            b_num = budget_label.find_next_sibling()
        else:
            b_num = ''
        resulting_row.append(int(''.join([i for i in b_num.text if i.isdigit()])) if b_num else None)
        gross_label = soup.find('span', class_='ipc-metadata-list-item__label', string='Gross worldwide')
        if gross_label:
            gross_num = gross_label.find_next_sibling()
        else: 
            gross_num = ''
        resulting_row.append(int(''.join([i for i in gross_num.text if i.isdigit()])) if gross_num else None)
    else:
        resulting_row.append(None)
        resulting_row.append(None)

    #7th - duration
    technical_specs = soup.find('section', {'data-testid': 'TechSpecs'})
    if technical_specs:
        duration_lable = technical_specs.find('span', class_='ipc-metadata-list-item__label', string='Runtime')
        if duration_lable:
            duration_string = duration_lable.find_next_sibling()
            if duration_string.text:
                try:
                    duration_list = duration_string.text.split()
                    duration = 0
                    if len(duration_list) == 2 and 'minutes' in duration_list:
                        duration = int(duration_list[0])
                    elif len(duration_list) == 2 and 'hours' in duration_list:
                        duration = int(duration_list[0]) * 60
                    else:
                        duration =  int(duration_list[0]) * 60 + int(duration_list[2])
                except IndexError:
                    duration = None
        else:
            duration = None
    else:
        duration = None
    resulting_row.append(duration)
    
    
    # Block for inserting film identifier
    full_cast = parse_movie_cast(url)
    if m_name:
        full_cast.insert(loc=0, column = 'movie_name', value = m_name.text)
    else:
        full_cast.insert(loc=0, column = 'movie_name', value = None)
    if year:
        full_cast.insert(loc=1, column = 'movie_duration', value = duration)
    else:
        full_cast.insert(loc=1, column = 'movie_duration', value = None)
    
    return resulting_row, full_cast

def table_extractor(lable_object):
    """helper for extracting dataframe of single role"""
    if lable_object:
        table = lable_object.find_next_sibling()
        table = pd.read_html(str(table))[0]
        table = table.replace('...', np.nan)
        substrings = ['Rest of cast listed alphabetically:', '(uncredited)']
        def contains_substring(cell):
            return any(substring in str(cell) for substring in substrings)
        filtered = table[~table.applymap(contains_substring).any(axis=1)]
        filtered = filtered.dropna(axis=1, how='all')
        filtered = filtered.dropna(axis=0, how='all')
        if len(filtered.columns) == 1:
            filtered['raw_role'] = None
        if len(filtered.columns) == 0:
            return pd.DataFrame()
        filtered.rename(columns={filtered.columns[0]: 'name', filtered.columns[1]: 'raw_role'}, inplace=True)
        return filtered
    else:
        return pd.DataFrame()
    
def parse_movie_cast(url):
    """collect single url cast data"""
    extended = '/fullcredits/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url+extended, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    director_lable = soup.find('h4', {'name': 'director', 'id': 'director', 'class': 'dataHeaderWithBorder'})
    pd_dir_table = table_extractor(director_lable)
    pd_dir_table['role'] = 'director'
    
    writer_lable = soup.find('h4', {'name': 'writer', 'id': 'writer', 'class': 'dataHeaderWithBorder'})
    pd_wr_table = table_extractor(writer_lable)
    pd_wr_table['role'] = 'writer'
    
    cast_lable = soup.find('h4', {'name': 'cast', 'id': 'cast', 'class': 'dataHeaderWithBorder'})
    filtered = table_extractor(cast_lable)
    filtered['role'] = 'actor'
    
    producer_lable = soup.find('h4', {'name': 'producer', 'id': 'producer', 'class': 'dataHeaderWithBorder'})
    pd_pr_table = table_extractor(producer_lable)
    pd_pr_table['role'] = 'producer'
    
    res = pd.concat([pd_dir_table, pd_wr_table], axis = 0)
    res = pd.concat([res, filtered], axis = 0)
    res = pd.concat([res, pd_pr_table], axis = 0)
    return res
    
def extract_links_list(rate_link, limit=99, filename=None):
    """parse rating links to list"""
    link_part = 'https://www.imdb.com'
    options = webdriver.ChromeOptions()
    options.add_argument("--lang=en-US")
    driver = webdriver.Chrome(options=options)
    driver.get(rate_link)
    for i in range(limit):
        button = driver.find_element(By.CSS_SELECTOR, '#__next > main > div.ipc-page-content-container.ipc-page-content-container--center.sc-d5064298-0.eMaGVc > div.ipc-page-content-container.ipc-page-content-container--center > section > section > div > section > section > div:nth-child(2) > div > section > div.ipc-page-grid.ipc-page-grid--bias-left.ipc-page-grid__item.ipc-page-grid__item--span-2 > div.ipc-page-grid__item.ipc-page-grid__item--span-2 > div.sc-619d2eab-0.dxnOGI > div > span > button')
        actions = ActionChains(driver)
        actions.move_to_element(button).perform()
        button.click()
        time.sleep(3)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    link_block = soup.find('ul', {'class': 'ipc-metadata-list ipc-metadata-list--dividers-between sc-748571c8-0 jApQAb detailed-list-view ipc-metadata-list--base', 'role': 'presentation'})
    links = link_block.find_all('a', class_='ipc-title-link-wrapper')
    link_list = [a['href'] for a in links if 'href' in a.attrs]
    mod_link_list = []
    for url in link_list:
        temp = url.split('/', 3)[:3]
        cut_temp = '/'.join(temp)
        mod_link_list.append(link_part + cut_temp)
    if filename == None:
        return mod_link_list
    else:
        with open(filename, 'w', encoding = 'utf-8') as file:
            file.write(str(mod_link_list))
        return mod_link_list

def process_link_to_file(link):
    """testing func, depricated"""
    logging.info('in process_link_func')
    row, dataframe = get_full_cast_from_link(link)
    logging.info('Processing link: %s', link)
    #later the folowing block must be reimplemented as querry to insert data
    with open('movie_data.csv', 'a', encoding='utf-8') as f_movie:
        csvwriter = csv.writer(f_movie)
        csvwriter.writerow(row)
    with open('actor_data.csv', 'a', encoding='utf-8') as f_actor:
        dataframe.to_csv(f_actor, mode='w', index = False, encoding='utf-8')
        f_actor.flush()
    logging.info('Data is written for link: %s', link)
    return row, dataframe

def process_item_to_database(database, user, password, host, row, dataframe):
    """Single url data insertion"""
    con = psycopg2.connect(database=database,
                           user=user,
                           password=password,
                           host=host)
    cur = con.cursor()

    for item in dataframe.itertuples(index=False, name=None):
        if item and item != ['movie_name', 'movie_duration', 'name', 'raw_role', 'role']:
            temp = [str(k) if k else None for k in item ]
            cur.execute("""insert into actor_raw_data_imdb (movie_name, movie_duration, name, raw_role, role)
                        values (%s, %s, %s, %s, %s)""", [i.replace("'", "") if isinstance(i, str) else i for i in item])
            con.commit()
    if row and isinstance(row, list):
        row = [str(i) if i else None for i in row]
        row = [j if isinstance(j, list) else j for j in row]
        cur.execute("""insert into movie_raw_data_imdb (movie_name, original_name, genres, year, url, certificate, budget, gross_worldwide, min_duration, rating)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", [row[1], row[2], row[6], row[3], row[0], row[4], row[7], row[8], row[9], row[5]])
        con.commit()
    con.commit()
    con.close()
    cur.close()
    return      
    
def full_parse_5000(rating_link, file_to_save_links_from_top=None, limit=99):
    """depricated"""
    link_list = extract_links_list(rating_link, limit, file_to_save_links_from_top)
    logging.info('Extracted %d links from rating link.', len(link_list))
    num_processes = multiprocessing.cpu_count()
    logging.info('Starting multiprocessing with %d processes.', num_processes)
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(process_link, link_list)

    return results
   
def single_pr_parse(rating_link, file_to_save_links_from_top=None, limit=99):
    """Testing function for sigle process parse"""
    link_list = extract_links_list(rating_link, limit, file_to_save_links_from_top)
    #link_list = link_list[0:10]
    count = 1
    for link in link_list:
        row, dataframe = get_full_cast_from_link(link)
        with open('movie_data.csv', 'a', encoding='utf-8') as f_movie:
            csvwriter = csv.writer(f_movie)
            csvwriter.writerow(row)
        with open('actor_data.csv', 'a', encoding='utf-8') as f_actor:
            if count == 1: 
                dataframe.to_csv(f_actor, mode='w', index = False, encoding='utf-8')
                f_actor.flush()
            else:
                dataframe.to_csv(f_actor, mode='a', index = False, header=False, encoding='utf-8')
                f_actor.flush()
        print(count)
        count += 1 
    return

def warehouse_top_insert(rating_link, file_to_save_links_from_top=None, limit=99, user='postgres', pwd=123):
    """Depricated"""
    con = psycopg2.connect(database="Films",
                           user=user,
                           password=pwd,
                           host="localhost")
    cur = con.cursor()
    link_list = extract_links_list(rating_link, limit)

    logging.info('Extracted %d links from rating link.', len(link_list))
    num_processes = multiprocessing.cpu_count()
    logging.info('Starting multiprocessing with %d processes.', num_processes)

    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(get_full_cast_from_link, link_list)
    con.commit()
    con.close()
    cur.close()
    return 'done'

def process_link(url):
    line, cast = get_full_cast_from_link(url)
    process_item_to_database(database, user, password, host, line, cast)
    return 1

def tab_truncate(database, user, password, host):
    con = psycopg2.connect(database=database,
                           user=user,
                           password=password,
                           host=host)
    cur = con.cursor()
    cur.execute("""truncate table public.movie_raw_data_imdb""")
    cur.execute("""truncate table public.actor_raw_data_imdb""")
    con.commit()
    con.close()
    cur.close()

def main(link_list, max_workers=4):
    count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_link, link_list))
    
    count = sum(results)
    #print(f'Total count: {count}')

rating = 'https://www.imdb.com/search/title/?title_type=feature&num_votes=5000,&sort=user_rating,desc'
limit = 99
database = 'Films'
password = '123'
host = 'localhost'
user = 'postgres'
tab_truncate(database, user, password, host)
link_list = extract_links_list(rating, limit)

main(link_list, max_workers=10)


