with temp1 as (
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
	join stg.movie_emp_link me on t.emp_movie_link_id = me.movie_emp_link_id