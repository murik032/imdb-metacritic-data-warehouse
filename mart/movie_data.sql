select
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
where mis.valid_to in (select max(valid_to) from stg.movie_info_sat)