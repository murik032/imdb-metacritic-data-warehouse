select md5(genre) 			as genre_id, 
	   genre 				as genre_nm 
from 
(select distinct
  replace(unnest(
    string_to_array(
      replace(replace(genres, '[', ''), ']', ''), ', '
    )
  ), '''', '') AS genre
from (
  select genres 
  from movie_raw_data_imdb mrd
  union all
  select genres
  from movie_raw_data_metacritic mrdm
) as combined_data)