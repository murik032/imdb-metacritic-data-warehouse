with temp1 as (
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
	join stg.movie_hub m on m.movie_id = i.movie_id