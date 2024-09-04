import psycopg2

def data_preproces(database, user, password, host):
    """preprocess data in public schema"""
    con = psycopg2.connect(database=database,
                           user=user,
                           password=password,
                           host=host)
    cur = con.cursor()
    cur.execute("""insert into public.actor_raw_data_imdb (movie_name, movie_duration, "name", raw_role, "role")
                select 
                    ai.movie_name 			as movie_name
                    , ai.movie_duration 		as movie_duration
                    , ai.raw_role				as "name"
                    , ai."role"					as raw_role
                    , ai."name"					as "role"
                from actor_raw_data_imdb ai 
                where ai."role" not in ('director', 'producer', 'writer', 'actor')""")
    cur.execute("""delete from public.actor_raw_data_imdb ai
                where ai."role" not in ('director', 'producer', 'writer', 'actor')""")
    con.commit()
    con.close()
    cur.close()


def etl_stg(database, user, password, host):
    """transfer data to stg dwh layer"""
    con = psycopg2.connect(database=database,
                           user=user,
                           password=password,
                           host=host)
    con.autocommit = True
    cur = con.cursor()

    cur.execute("""call stg_checker('stg', 'genre_hub');""")
    cur.execute("""call stg_checker('stg', 'employee_hub');""")
    cur.execute("""call stg_checker('stg', 'movie_hub');""")
    cur.execute("""call stg_checker('stg', 'movie_info_sat');""")
    cur.execute("""call stg_checker('stg', 'movie_genre_link');""")
    cur.execute("""call stg_checker('stg', 'movie_emp_link');""")
    cur.execute("""call stg_checker('stg', 'emp_movie_l_sat');""")

    con.commit()
    con.close()
    cur.close()


def etl_data_mart(database, user, password, host):
    """transfer data to the data mart"""
    con = psycopg2.connect(database=database,
                           user=user,
                           password=password,
                           host=host)
    con.autocommit = True
    cur = con.cursor()

    cur.execute("""call stg_checker('data_mart', 'employee_data');""")
    cur.execute("""call stg_checker('data_mart', 'movie_data');""")
    cur.execute("""call stg_checker('data_mart', 'movie_employee_link');""")
    cur.execute("""call stg_checker('data_mart', 'genre_metrics');""")
    cur.execute("""call stg_checker('data_mart', 'rating_slide');""")

    con.commit()
    con.close()
    cur.close()


database = 'Films'
user = 'postgres'
password = '123'
host = 'localhost'

data_preproces(database, user, password, host)
etl_stg(database, user, password, host)
etl_data_mart(database, user, password, host)