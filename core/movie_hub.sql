with temp1 as (
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
where md5(movie_nm||movie_duration) is not null