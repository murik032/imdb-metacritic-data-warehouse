with get_films as (
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
select distinct md5(movie_id||url) as title_item_id, * from get_movie_id