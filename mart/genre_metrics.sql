with temp1 as (
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
order by genre_movie_quant desc