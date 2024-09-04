import psycopg2

def dwh_ddl(database, user, password, host):
    """This function executes all needed ddl operations in empty database and inserts all nessesery metadata in meta schema"""
    con = psycopg2.connect(database=database,
                            user=user,
                            password=password,
                            host=host)
    cur = con.cursor()

    cur.execute("""create schema if not exists public""")
    cur.execute("""create schema if not exists stg""")
    cur.execute("""create schema if not exists data_mart""")
    cur.execute("""create schema if not exists meta""")

    cur.execute("""CREATE TABLE public.actor_raw_data_imdb (
                    movie_name varchar NULL,
                    movie_duration int4 NULL,
                    "name" varchar NULL,
                    raw_role varchar NULL,
                    "role" varchar NULL
                )""")
    cur.execute("""CREATE TABLE if not exists public.movie_raw_data_imdb (
                    url varchar NULL,
                    movie_name varchar NULL,
                    original_name varchar NULL,
                    "year" varchar NULL,
                    certificate varchar NULL,
                    rating varchar NULL,
                    genres varchar NULL,
                    budget varchar NULL,
                    gross_worldwide varchar NULL,
                    min_duration varchar NULL
                )""")
    cur.execute("""CREATE TABLE public.actor_raw_data_metacritic (
                    movie_name varchar NULL,
                    movie_duration int4 NULL,
                    "name" varchar NULL,
                    raw_role varchar NULL,
                    "role" varchar NULL
                )""")
    cur.execute("""CREATE TABLE if not exists public.movie_raw_data_metacritic (
                    url varchar NULL,
                    movie_name varchar NULL,
                    original_name varchar NULL,
                    "year" varchar NULL,
                    certificate varchar NULL,
                    rating varchar NULL,
                    genres varchar NULL,
                    budget varchar NULL,
                    gross_worldwide varchar NULL,
                    min_duration varchar NULL
                )""")
    cur.execute("""create table if not exists meta.etl_tab_script (
                    sch_name varchar,
                    tab_name varchar,
                    script text
                )""")
    cur.execute("""create table if not exists meta.etl_col (
                    scn_nm varchar,
                    tab_nm varchar,
                    col_nm text,
                    cnstr varchar(2) check (cnstr in ('pk', 'n', 'fk')),
                    d_type varchar
                )""")
    
    cur.execute("""insert into meta.etl_tab_script 
                values
                    ('stg', 'emp_movie_l_sat', $$with temp1 as (
                    select distinct
                            movie_name, 
                            cast(movie_duration as int) 		as dur, 
                            name, 
                            raw_role, 
                            role 
                    from public.actor_raw_data_imdb
                    union
                    select  distinct
                            movie_name, 
                            cast(movie_duration as int) 		as dur, 
                            name, 
                            raw_role, 
                            role 
                    from public.actor_raw_data_metacritic
                ),
                temp2 as (
                    select 
                            md5(movie_name||dur) 				as movie_id, 
                            md5(name) 							as emp_id, 
                            raw_role, 
                            role 
                    from temp1
                ),
                temp3 as (
                    select distinct
                        md5(movie_id||emp_id)					as emp_movie_link_id,
                        movie_id,
                        emp_id,
                        raw_role,
                        role
                from temp2
                )
                select distinct 
                    md5(me.movie_emp_link_id||t.raw_role||t.role) 		as movie_emp_role_id,
                    me.movie_emp_link_id,
                    t.raw_role									as description,
                    t.role
                from temp3 t
                    join stg.movie_emp_link me on t.emp_movie_link_id = me.movie_emp_link_id$$),
                    ('stg', 'employee_hub', $$with temp1 as (
                    select * 
                    from public.actor_raw_data_imdb
                    union
                    select * 
                    from public.actor_raw_data_metacritic
                ), 
                temp2 as (
                    select distinct
                        md5("name")			as emp_id
                        , "name"				as emp_nm
                    from temp1
                )
                select * from temp2$$),
                    ('stg', 'genre_hub', $$select md5(genre) 			as genre_id, 
                    genre 				as genre_nm 
                from 
                (select distinct
                replace(unnest(
                    string_to_array(
                    replace(replace(genres, '[', ''), ']', ''), ', '
                    )
                ), '''', '') AS genre
                from (
                select genres 
                from movie_raw_data_imdb mrd
                union all
                select genres
                from movie_raw_data_metacritic mrdm
                ) as combined_data)$$),
                    ('stg', 'movie_emp_link', $$with temp1 as (
                    select 
                        movie_name 							as movie_nm
                        , cast(movie_duration as int) 			as duration
                        ,"name"
                    from public.actor_raw_data_imdb 
                    union 
                    select 
                        movie_name 							as movie_nm
                        , cast(movie_duration as int) 			as duration
                        ,"name"
                    from public.actor_raw_data_metacritic
                ),
                temp2 as (
                    select
                        md5(t1.movie_nm||t1.duration) 		as movie_id
                        , md5("name")							as emp_id
                    from temp1 t1
                    where md5(t1.movie_nm||t1.duration) is not null
                )
                select distinct 
                    md5(m.movie_id||j.emp_id)					as movie_emp_link_id,
                    m.movie_id 									as movie_id,
                    j.emp_id 									as emp_id		
                from temp2 i 
                    join stg.employee_hub j on i.emp_id = j.emp_id 
                    join stg.movie_hub m on m.movie_id = i.movie_id$$),
                    ('stg', 'movie_genre_link', $$with temp1 as (
                    select
                        movie_name														as movie_nm
                        , cast(min_duration as int) 									as duration
                        , json_array_elements_text(replace(genres, '''', '"')::json) 	as genres
                    from public.movie_raw_data_imdb
                    union
                    select
                        movie_name														as movie_nm
                        , cast(min_duration as int) 									as duration
                        , json_array_elements_text(replace(genres, '''', '"')::json) 	as genres
                    from public.movie_raw_data_metacritic
                )
                select distinct 
                        md5(j.movie_id||g.genre_id)										as  mv_gen_link_id
                    , j.movie_id														as movie_id
                    , g.genre_id 														as genre_id
                from temp1 i join stg.movie_hub j on i.movie_nm = j.movie_nm and i.duration = j.movie_duration join stg.genre_hub g on g.genre_nm = i.genres$$),
                    ('stg', 'movie_hub', $$with temp1 as (
                    select 
                        movie_name						as movie_nm
                        , cast(min_duration as int)			as movie_duration
                    from public.movie_raw_data_imdb
                    union
                    select  
                        movie_name						as movie_nm
                        , cast(min_duration as int)			as movie_duration 
                    from public.movie_raw_data_metacritic
                )
                select distinct 
                    md5(movie_nm||movie_duration)			as movie_id
                    , movie_nm
                    , movie_duration
                from temp1 
                where md5(movie_nm||movie_duration) is not null$$),
                    ('stg', 'movie_info_sat', $$with get_films as (
                select distinct
                    movie_name
                    , cast(min_duration as int) as duration 
                    , original_name
                    , "year"
                    , certificate
                    , rating
                    , budget
                    , gross_worldwide
                    , 'IMDB' as SCR_NM
                    , url
                from 
                    public.movie_raw_data_imdb
                
                union
                
                select  distinct
                    movie_name
                    , cast(min_duration as int) as duration 
                    , original_name
                    , "year"
                    , certificate
                    , rating
                    , budget
                    , gross_worldwide
                    , 'METACRITIC' as SCR_NM
                    , url
                from 
                    public.movie_raw_data_metacritic
                ), 
                get_movie_id as (
                select 
                    t2.movie_id
                    , t1.original_name,t1."year",t1.certificate,t1.rating,t1.budget,t1.gross_worldwide,t1.scr_nm,url
                from 
                    get_films t1
                    join stg.movie_hub t2
                    on md5(t1.movie_name||t1.duration) = t2.movie_id      
                )
                select distinct md5(movie_id||url) as title_item_id, * from get_movie_id$$),
                    ('data_mart', 'employee_data', $$select distinct
                    emls.movie_emp_role_id,
                    eh.emp_nm as name,
                    emls.role as role, 
                    case when emls.description = 'NaN' then ''
                        else replace(replace(emls.description, ')', ''), '(', '')
                    end as role_description
                from stg.employee_hub eh 
                    join stg.movie_emp_link mel on eh.emp_id = mel.emp_id
                    join stg.emp_movie_l_sat emls on mel.movie_emp_link_id = emls.movie_emp_link_id
                where emls.valid_to in (select max(valid_to) from stg.emp_movie_l_sat)
                    and mel.valid_to in (select max(valid_to) from stg.movie_emp_link)$$),
                    ('data_mart', 'genre_metrics', $$with temp1 as (
                    select max(valid_from) 
                    from stg.movie_info_sat
                ),
                temp2 as (
                    select 
                        mis.movie_id,
                        avg(cast(mis.rating as float)) as rating,
                        avg(cast(mis.budget as bigint)) as budget,
                        avg(mis.gross_worldwide::bigint) as gross_worldwide
                    from stg.movie_info_sat mis 
                    group by mis.movie_id
                ),
                temp3 as (
                    select 
                        gh.genre_id,
                        mh.movie_id,
                        mh.movie_nm,
                        mh.movie_duration, 
                        t2.rating, 
                        t2.budget,
                        t2.gross_worldwide,
                        gh.genre_nm
                    from temp2 t2 
                        join stg.movie_hub mh on mh.movie_id = t2.movie_id
                        join stg.movie_genre_link mgl on t2.movie_id = mgl.movie_id
                        join stg.genre_hub gh on gh.genre_id = mgl.genre_id
                ),
                temp_max_budget as (
                    select 
                        genre_nm,
                        max(budget) as budget
                    from temp3 
                    group by genre_nm
                ),
                temp_max_rating as (
                    select 
                        genre_nm,
                        max(rating) as rating
                    from temp3
                    group by genre_nm
                ),
                temp_max_gross as (
                    select 
                        genre_nm,
                        max(gross_worldwide) as gross
                    from temp3
                    group by genre_nm
                )
                select 
                    genre_id,
                    genre_nm as genre,
                    (select movie_nm||', '||movie_duration||' min'
                    from temp3 as m1 where m1.genre_nm = m.genre_nm
                    order by budget desc limit 1) as max_budget_movie,
                    (select movie_nm||', '||movie_duration||' min'
                    from temp3 as m1 where m1.genre_nm = m.genre_nm 
                    order by gross_worldwide desc limit 1) as max_gross_movie,
                    (select movie_nm||', '||movie_duration||' min'
                    from temp3 as m1 where m1.genre_nm = m.genre_nm
                    order by rating desc limit 1) as best_rated_movie,
                    avg(rating) as average_rating,
                    count(movie_id) as genre_movie_quant
                from temp3 as m
                group by genre, genre_id
                order by genre_movie_quant desc$$),
                    ('data_mart', 'movie_data', $$select
                    mis.title_item_id,
                    mh.movie_nm as movie_name,
                    mh.movie_duration,
                    coalesce(mis.original_name, '') as original_name,
                    coalesce(mis.year, '') as year,
                    mis.rating,
                    coalesce(mis.budget, '') as budget,
                    coalesce(mis.gross_worldwide, '') as worldwide_gross,
                    scr_nm as rating_source,
                    url
                from stg.movie_hub mh 
                    join stg.movie_info_sat mis on mh.movie_id = mis.movie_id
                where mis.valid_to in (select max(valid_to) from stg.movie_info_sat)$$),
                    ('data_mart', 'movie_employee_link', $$select distinct
                    mel.movie_emp_link_id,
                    mh.movie_nm,
                    mh.movie_duration,
                    eh.emp_nm
                from stg.movie_hub mh
                    join stg.movie_emp_link mel on mh.movie_id = mel.movie_id
                    join stg.employee_hub eh on mel.emp_id = eh.emp_id$$),
                    ('data_mart', 'rating_slide', $$with temp1 as (
                    select
                        mh.movie_nm,
                        mh.movie_id,
                        cast(mh.movie_duration as int),
                        avg(rating::float) as rating,
                        row_number() over (order by avg(rating::float) desc, cast(mh.movie_duration as int) desc) as latest_place
                    from stg.movie_hub mh 
                        join stg.movie_info_sat mis on mh.movie_id = mis.movie_id 
                    where mis.valid_from in (select max(valid_from) from stg.movie_info_sat) 	
                    group by movie_nm, movie_duration, mh.movie_id
                    order by avg(rating::float) desc, cast(mh.movie_duration as int) desc
                ),
                temp2 as (
                    select  distinct 
                        mh.movie_nm,
                        mh.movie_id,
                        cast(mh.movie_duration as int),
                        avg(rating::float) as rating,
                        row_number() over (order by avg(rating::float) desc, cast(mh.movie_duration as int) desc) as pre_latest_place
                    from stg.movie_hub mh 
                        join stg.movie_info_sat mis on mh.movie_id = mis.movie_id 
                    where mis.valid_from in ((select max(valid_from) from stg.movie_info_sat where valid_from < (select max(valid_from) from stg.movie_info_sat)), (select max(valid_from) from stg.movie_info_sat))
                    group by movie_nm, movie_duration, mh.movie_id
                    order by avg(rating::float) desc, cast(mh.movie_duration as int) desc 
                )
                select 
                    coalesce(t1.movie_id, t2.movie_id) as movie_id,
                    t1.movie_nm as movie_name,
                    t1.movie_duration as duration,
                    t1.rating as current_rating,
                    t1.latest_place as current_place
                from temp1 t1 full join 
                    temp2 t2 on t1.movie_id = t2.movie_id
                order by current_place$$)""")
    cur.execute("""insert into meta.etl_col 
                values
                    ('stg', 'emp_movie_l_sat', 'movie_emp_role_id', 'pk', 'text'),
                    ('stg', 'emp_movie_l_sat', 'movie_emp_link_id', 'n', 'text'),
                    ('stg', 'emp_movie_l_sat', 'description', 'n', 'varchar'),
                    ('stg', 'emp_movie_l_sat', 'role', 'n', 'varchar'),
                    ('stg', 'employee_hub', 'emp_id', 'pk', 'text'),
                    ('stg', 'employee_hub', 'emp_nm', 'n', 'varchar'),
                    ('stg', 'genre_hub', 'genre_id', 'pk', 'text'),
                    ('stg', 'genre_hub', 'genre_nm', 'n', 'text'),
                    ('stg', 'movie_emp_link', 'movie_emp_link_id', 'pk', 'text'),
                    ('stg', 'movie_emp_link', 'movie_id', 'n', 'text'),
                    ('stg', 'movie_emp_link', 'emp_id', 'n', 'text'),
                    ('stg', 'movie_genre_link', 'mv_gen_link_id', 'pk', 'text'),
                    ('stg', 'movie_genre_link', 'movie_id', 'n', 'text'),
                    ('stg', 'movie_genre_link', 'genre_id', 'n', 'text'),
                    ('stg', 'movie_hub', 'movie_id', 'pk', 'text'),
                    ('stg', 'movie_hub', 'movie_nm', 'n', 'varchar'),
                    ('stg', 'movie_hub', 'movie_duration', 'n', 'int'),
                    ('stg', 'movie_info_sat', 'title_item_id', 'pk', 'text'),
                    ('stg', 'movie_info_sat', 'movie_id', 'n', 'text'),
                    ('stg', 'movie_info_sat', 'original_name', 'n', 'varchar'),
                    ('stg', 'movie_info_sat', 'year', 'n', 'varchar'),
                    ('stg', 'movie_info_sat', 'certificate', 'n', 'varchar'),
                    ('stg', 'movie_info_sat', 'rating', 'n', 'varchar'),
                    ('stg', 'movie_info_sat', 'budget', 'n', 'varchar'),
                    ('stg', 'movie_info_sat', 'gross_worldwide', 'n', 'varchar'),
                    ('stg', 'movie_info_sat', 'scr_nm', 'n', 'text'),
                    ('stg', 'movie_info_sat', 'url', 'n', 'varchar'),
                    ('stg', 'emp_movie_l_sat', 'valid_from', 'n', 'timestamp'),
                    ('stg', 'emp_movie_l_sat', 'valid_to', 'n', 'timestamp'),
                    ('stg', 'movie_emp_link', 'valid_from', 'n', 'timestamp'),
                    ('stg', 'movie_emp_link', 'valid_to', 'n', 'timestamp'),
                    ('stg', 'movie_genre_link', 'valid_from', 'n', 'timestamp'),
                    ('stg', 'movie_genre_link', 'valid_to', 'n', 'timestamp'),
                    ('stg', 'movie_info_sat', 'valid_from', 'n', 'timestamp'),
                    ('stg', 'movie_info_sat', 'valid_to', 'n', 'timestamp'),
                    ('data_mart', 'movie_employee_link', 'movie_emp_link_id', 'pk', 'text'),
                    ('data_mart', 'movie_employee_link', 'movie_nm', 'n', 'varchar'),
                    ('data_mart', 'movie_employee_link', 'movie_duration', 'n', 'int'),
                    ('data_mart', 'movie_employee_link', 'emp_nm', 'n', 'varchar'),
                    ('data_mart', 'employee_data', 'movie_emp_role_id', 'pk', 'text'),
                    ('data_mart', 'employee_data', 'name', 'n', 'varchar'),
                    ('data_mart', 'employee_data', 'role', 'n', 'varchar'),
                    ('data_mart', 'employee_data', 'role_description', 'n', 'text'),
                    ('data_mart', 'genre_metrics', 'genre_id', 'pk', 'text'),
                    ('data_mart', 'genre_metrics', 'genre', 'n', 'text'),
                    ('data_mart', 'genre_metrics', 'max_budget_movie', 'n', 'text'),
                    ('data_mart', 'genre_metrics', 'max_gross_movie', 'n', 'text'),
                    ('data_mart', 'genre_metrics', 'best_rated_movie', 'n', 'text'),
                    ('data_mart', 'genre_metrics', 'average_rating', 'n', 'float'),
                    ('data_mart', 'genre_metrics', 'genre_movie_quant', 'n', 'int'),
                    ('data_mart', 'movie_data', 'title_item_id', 'pk', 'text'),
                    ('data_mart', 'movie_data', 'movie_name', 'n', 'varchar'),
                    ('data_mart', 'movie_data', 'movie_duration', 'n', 'int'),
                    ('data_mart', 'movie_data', 'original_name', 'n', 'varchar'),
                    ('data_mart', 'movie_data', 'year', 'n', 'varchar'),
                    ('data_mart', 'movie_data', 'rating', 'n', 'varchar'),
                    ('data_mart', 'movie_data', 'budget', 'n', 'varchar'),
                    ('data_mart', 'movie_data', 'worldwide_gross', 'n', 'varchar'),
                    ('data_mart', 'movie_data', 'rating_source', 'n', 'text'),
                    ('data_mart', 'movie_data', 'url', 'n', 'varchar'),
                    ('data_mart', 'rating_slide', 'movie_id', 'pk', 'text'),
                    ('data_mart', 'rating_slide', 'movie_name', 'n', 'varchar'),
                    ('data_mart', 'rating_slide', 'duration', 'n', 'int'),
                    ('data_mart', 'rating_slide', 'current_rating', 'n', 'float'),
                    ('data_mart', 'rating_slide', 'current_place', 'n', 'int')""")
    
    
    cur.execute("""create or replace procedure create_table(schema_name varchar, table_name varchar)
                language plpgsql
                as $$
                declare script_as_txt varchar;
                    col_data varchar;
                begin
                    begin
                        select string_agg(col_nm||' '||d_type, ', ') from meta.etl_col WHERE scn_nm = schema_name AND tab_nm = table_name
                        into col_data;
                        select 'create table '||schema_name||'.'||table_name||' (' ||col_data||' );' 
                        into script_as_txt;
                        raise notice '%', script_as_txt;
                    execute script_as_txt;
                    commit;
                    end;  
                end $$""")
    cur.execute("""create or replace procedure stg_checker(schema_name varchar, tb_nm varchar)
                language plpgsql
                as $$
                declare script_as_txt varchar;
                    temp_table_script varchar;
                    ref_column record;
                    real_column record;
                    column_exists boolean;
                    temp123 varchar;
                    temp_dtype_updt_scr varchar;
                    col_nm_temp record;
                    col_drop_scr text;
                    valid_to_update text;
                    cl_array text;
                    key_col text;
                    temp_table_update text;
                    insert_script text;
                    temp_table_insert_scr text;
                    dynamic_query text;
                    inequality text;
                    data_table_dates text;
                    temp_tb_insertion text;
                begin
                    if not exists (select 1 from pg_catalog.pg_tables
                            where schemaname = schema_name and tablename = tb_nm) then
                        begin
                            call create_table(schema_name, tb_nm);
                        end;  
                    end if;
                    for ref_column in
                        select col_nm, d_type from meta.etl_col where scn_nm = schema_name and tab_nm = tb_nm
                    loop	
                        select exists (select 1 from information_schema.columns 
                        where table_schema = schema_name and table_name = tb_nm and column_name = ref_column.col_nm)
                        into column_exists;
                        if not column_exists then
                            select 'alter table '||schema_name||'.'||tb_nm||' add column '||ref_column.col_nm||' '||ref_column.d_type||';'
                            into temp123;
                            execute temp123;
                        end if;
                    end loop;
                    for ref_column in
                        select col_nm, d_type from meta.etl_col where scn_nm = schema_name and tab_nm = tb_nm
                    loop
                        select 'alter table '||schema_name||'.'||tb_nm||' alter column '||ref_column.col_nm||' type '||ref_column.d_type||' using '||ref_column.col_nm||'::'||ref_column.d_type||';'
                        into temp_dtype_updt_scr;
                        execute temp_dtype_updt_scr;
                    end loop;
                    for ref_column in
                        select column_name from information_schema.columns where table_name = tb_nm and table_schema = schema_name
                    loop 
                        if replace(replace(cast(ref_column as text), '(', ''), ')', '') not in (select cast(col_nm as text) from meta.etl_col where tab_nm = tb_nm and scn_nm = schema_name) then
                            select 'alter table '||schema_name||'.'||tb_nm||' drop column '||replace(replace(cast(ref_column as text), '(', ''), ')', '')||';'
                            into col_drop_scr;
                            execute col_drop_scr;
                            commit;
                        end if;			  
                    end loop;
                    call temp_table(schema_name, tb_nm);
                    select col_nm from meta.etl_col where tab_nm = tb_nm and scn_nm = schema_name and cnstr = 'pk'
                    into key_col;
                    select string_agg(col_nm, ', ') from meta.etl_col where tab_nm = tb_nm and scn_nm = schema_name and cnstr != 'pk' and col_nm not in ('valid_to', 'valid_from')
                    into cl_array;
                    if tb_nm not like '%'||'hub'||'%' and schema_name <> 'data_mart' then
                        select 'alter table temp_ add column valid_from timestamp default current_timestamp, add column valid_to timestamp default ''9999-12-31'''
                        into temp_table_update;
                        execute temp_table_update;
                        commit;
                    
                        select string_agg(format('l.%s != t.%s', col_name, col_name), ' or ' )
                        into inequality
                        from unnest(string_to_array(cl_array, ', ')) as col_name;
                        --raise notice '%', inequality;
                    
                        select 'update '||schema_name||'.'||tb_nm||' l set valid_to = current_timestamp 
                                where '||key_col||' in (select l.'||key_col||' from '||schema_name||'.'||tb_nm||' l left join temp_ t on l.'||key_col||' = t.'||key_col||' where (l.valid_to = ''9999-12-31'' and (t.'||key_col||' is null or '||inequality||')))'
                        into dynamic_query;
                        --raise notice '%', dynamic_query;
                        execute dynamic_query;
                        commit;
                        
                        select 'insert into '||schema_name||'.'||tb_nm||' ('||key_col||', '||cl_array||', valid_from, valid_to)
                                select '||key_col||', '||cl_array||', valid_from, valid_to from temp_ where '||key_col||' in (select t.'||key_col||' from temp_ t left join '||schema_name||'.'||tb_nm||' l on t.'||key_col||' = l.'||key_col||' where (l.'||key_col||' is null or '||inequality||'))'
                        into temp_tb_insertion;
                        --raise notice '%', temp_tb_insertion;
                        execute temp_tb_insertion;
                        commit;
                    
                    else
                        select 'insert into '||schema_name||'.'||tb_nm||' ('||key_col||', '||cl_array||') select '||key_col||', '||cl_array||' from temp_ where '||key_col||' not in (select distinct '||key_col||' from '||schema_name||'.'||tb_nm||');'
                        into insert_script;
                        execute insert_script;
                        commit;
                    end if;
                    drop table temp_;
                end $$""")
    cur.execute("""create or replace procedure temp_table(sc_nm varchar, table_name varchar)
                language plpgsql
                as $$ 
                declare script_as_txt varchar;
                    temp_table_script varchar;
                begin 
                    select script from meta.etl_tab_script where sch_name = sch_name and tab_name = table_name
                    into script_as_txt;
                    select 'create temp table temp_ as '||script_as_txt||' ;'
                    into temp_table_script;
                    execute temp_table_script;
                end $$""")
    con.commit()
    con.close()
    cur.close()


database = 'Films'
user = 'postgres'
password = '123'
host = 'localhost'

dwh_ddl(database, user, password, host)







