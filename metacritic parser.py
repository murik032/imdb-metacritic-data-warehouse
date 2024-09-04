import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import psycopg2
from functools import partial
import concurrent.futures

def get_link_from_metascore(rating_url, limit=209):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
    }
    
    link_list = []
    for page_num in range(limit):
        url = rating_url + str(page_num)
        response = requests.get(url, headers=headers)
        html = BeautifulSoup(response.content, 'html.parser')
        links = html.find_all('a', attrs={'class': 'c-finderProductCard_container g-color-gray80 u-grid'})
        for i in links:
            link_list.append(i.get('href'))
            print(len(link_list))
    return link_list

def single_link_process(url):
    movie_data = []
    genres_tag = None
    duration_tag = None
    movie_data.append(url)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
    }
    response = requests.get(url, headers=headers)
    html = BeautifulSoup(response.content, 'html.parser')
    
    #1st - movie name
    name = html.find('h1')
    if name:
        movie_data.append(name.text)
    else:
        movie_data.append(None)
    
    #Original name is not presented
    movie_data.append(None)
    
    #2nd - year and certificate
    lable_block = html.find('div', class_='c-ProductionDetails')
    res_date = None
    cert = None
    if lable_block:
        date_lable = lable_block.find_all('span', class_='g-text-bold')
        if date_lable:
            for i in date_lable:
                if i.text == 'Release Date':
                    date = i.find_next_sibling()
                    date_part_list = [j for j in date.text.split(' ')]
                    if date_part_list[-1].isdigit() and len(date_part_list[-1]) == 4:
                        res_date = int(date_part_list[-1])
                if i.text == 'Rating':
                    cert_tag = i.find_next_sibling()
                    cert = cert_tag.text
                if i.text == 'Genres':
                    genres_tag = i.find_next_sibling()
                if i.text == 'Duration':
                    duration_tag = i.find_next_sibling()
                    
    movie_data.append(res_date)
    movie_data.append(cert)
    
    #3rd - metascore rating / 10 to make it simmilar to imdb
    
    rating = None
    rating_cell = html.find('div', class_='c-productHero_score-container u-flexbox u-flexbox-column g-bg-white')

    if rating_cell:
        rating_cell2 = rating_cell.find('div', class_='c-productScoreInfo u-clearfix g-inner-spacing-bottom-medium')
        if rating_cell2:
            rating_cell3 = rating_cell2.find('div', class_='c-siteReviewScore u-flexbox-column u-flexbox-alignCenter u-flexbox-justifyCenter g-text-bold c-siteReviewScore_green g-color-gray90 c-siteReviewScore_medium')
            if rating_cell3:
                rating_num = rating_cell3.find('span', {'data-v-e408cafe': ''})
                if rating_num:
                    rating = int(rating_num.text) / 10
    movie_data.append(rating)
    
    #4th - genres list
    
    genres_list = None
    if genres_tag:
        genres_cells_list = genres_tag.find_all('span', class_ = 'c-globalButton_label')
        if genres_cells_list:
            genres_list = []
            for cell in genres_cells_list:
                genres_list.append(cell.text.strip())
    movie_data.append(genres_list)
    
    #5th - budget and groos are not presented
    
    movie_data.append(None)
    movie_data.append(None)
    
    #6th - duration in minutes
    
    duration = None
    if duration_tag:
        duration_string = duration_tag.text
        duration_list = [n for n in duration_string.split(' ')]
        if len(duration_list) == 2:
            if 'h' in duration_list:
                duration = int(duration_list[0]) * 60
            else:
                duration = int(duration_list[0])
        elif len(duration_list) == 4:
            duration = int(duration_list[0]) * 60 + int(duration_list[2])
    movie_data.append(duration)
    
    actor_data = get_full_cast(url)
    if name:
        actor_data.insert(loc=0, column = 'movie_name', value = name.text)
    else:
        actor_data.insert(loc=0, column = 'movie_name', value = None)
    if res_date:
        actor_data.insert(loc=1, column = 'movie_duration', value = duration)
    else:
        actor_data.insert(loc=1, column = 'movie_duration', value = None)
    
    return movie_data, actor_data

def get_full_cast(url):
    cast = pd.DataFrame()
    link = url + 'credits/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
    }
    response = requests.get(link, headers=headers)
    credits_html = BeautifulSoup(response.content, 'html.parser')

    header_list = credits_html.find_all('h3', class_='c-productCredits_groupName g-color-gray80 g-text-bold u-text-uppercase')
    if header_list:
        for i in header_list:
            if i.text.strip() == 'Cast':
                table_tag = i.find_next_sibling()
                table = extract_metacritic_table(table_tag)
                table['role'] = 'actor'
                cast = pd.concat([table, cast], axis = 0)
            elif i.text.strip() == 'Written By':
                table_tag = i.find_next_sibling()
                table = extract_metacritic_table(table_tag)
                table['role'] = 'writer'
                cast = pd.concat([table, cast], axis = 0)
            elif i.text.strip() == 'Directed By':
                table_tag = i.find_next_sibling()
                table = extract_metacritic_table(table_tag)
                table['role'] = 'director'
                cast = pd.concat([table, cast], axis = 0)
            elif i.text.strip() == 'Produced By':
                table_tag = i.find_next_sibling()
                table = extract_metacritic_table(table_tag)
                table['role'] = 'producer'
                cast = pd.concat([table, cast], axis = 0)

    return cast
                
def extract_metacritic_table(table_html):
    names = []
    raw_roles = []

    if table_html:
        row_tags = table_html.find_all('div', class_='u-grid-3column g-inner-spacing-medium')
        for i in row_tags:
            name_tag = i.find('dd')
            raw_role_tag = i.find('dt')
            name = name_tag.get_text(strip=True) if name_tag else None
            raw_role = raw_role_tag.get_text(strip=True) if raw_role_tag else None
            names.append(name)
            raw_roles.append(raw_role)
    table_dict = {
        'name': names,
        'raw_role': raw_roles      
    }
    table = pd.DataFrame(table_dict)
    return table

def meta_process(url):
    global database
    global user
    global password
    global host
    row, frame = single_link_process(url)
    process_link_to_database(database, user, password, host, row, frame)
    return 1
    
def process_link_to_database(database, user, password, host, row, dataframe):
    con = psycopg2.connect(database=database,
                           user=user,
                           password=password,
                           host=host)
    cur = con.cursor()
    for item in dataframe.itertuples(index=False, name=None):
        if item and item != ['movie_name', 'movie_duration', 'name', 'raw_role', 'role']:
            temp = [str(k) if k else None for k in item ]
            cur.execute("""insert into actor_raw_data_metacritic (movie_name, movie_duration, name, raw_role, role)
                        values (%s, %s, %s, %s, %s)""", [i.replace("'", "") if isinstance(i, str) else i for i in item])
            con.commit()
    if row and isinstance(row, list):
        row = [str(i) if i else None for i in row]
        row = [j if isinstance(j, list) else j for j in row]
        cur.execute("""insert into movie_raw_data_metacritic (movie_name, original_name, genres, year, url, certificate, budget, gross_worldwide, min_duration, rating)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", [row[1], row[2], row[6], row[3], row[0], row[4], row[7], row[8], row[9], row[5]])
        con.commit()
    con.commit()
    con.close()
    cur.close()
    return    

def tab_truncate(database, user, password, host):
    con = psycopg2.connect(database=database,
                           user=user,
                           password=password,
                           host=host)
    cur = con.cursor()
    cur.execute("""truncate table public.movie_raw_data_metacritic""")
    cur.execute("""truncate table public.actor_raw_data_metacritic""")
    con.commit()
    con.close()
    cur.close()

def main(link_list, max_workers=12):
    count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        result = list(executor.map(meta_process, link_list))
    count = sum(result)
    print(f'Count:{count}')

rating_url = 'https://www.metacritic.com/browse/movie/?releaseYearMin=1910&releaseYearMax=2024&page='
limit = 210
link_list = get_link_from_metascore(rating_url, limit)
#with open('temporary.txt', 'w', encoding = 'utf-8') as file:
#    file.write(str(link_list))
database = 'Films'
user = 'postgres'
password = '123'
host = 'localhost'

link_list = ['https://www.metacritic.com'+ i for i in link_list]
print(len(link_list))
#print(len(link_list))
tab_truncate(database, user, password, host)
main(link_list, max_workers=10)
