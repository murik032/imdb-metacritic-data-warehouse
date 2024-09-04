with temp1 as (
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
order by current_place