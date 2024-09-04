with temp1 as (
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
select * from temp2