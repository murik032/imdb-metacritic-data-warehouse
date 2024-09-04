with temp1 as (
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
from temp1 i join stg.movie_hub j on i.movie_nm = j.movie_nm and i.duration = j.movie_duration join stg.genre_hub g on g.genre_nm = i.genres