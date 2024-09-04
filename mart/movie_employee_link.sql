select distinct
	mel.movie_emp_link_id,
	mh.movie_nm,
	mh.movie_duration,
	eh.emp_nm
from stg.movie_hub mh
	join stg.movie_emp_link mel on mh.movie_id = mel.movie_id
	join stg.employee_hub eh on mel.emp_id = eh.emp_id