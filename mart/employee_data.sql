select distinct
	emls.movie_emp_role_id,
	eh.emp_nm as name,
	emls.role as role, 
	case when emls.description = 'NaN' then ''
		else replace(replace(emls.description, ')', ''), '(', '')
	end as role_description
from stg.employee_hub eh 
	join stg.movie_emp_link mel on eh.emp_id = mel.emp_id
	join stg.emp_movie_l_sat emls on mel.movie_emp_link_id = emls.movie_emp_link_id
where emls.valid_to in (select max(valid_to) from stg.emp_movie_l_sat)
	and mel.valid_to in (select max(valid_to) from stg.movie_emp_link)