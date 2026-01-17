select
 __p_name as name
 ,case when (__p_age) < (18) then 'minor' when (__p_age) >= (65) then 'senior' else 'adult' end as ageGroup
from (
 select
 id as __p_id
 ,name as __p_name
 ,age as __p_age
 from
 graph.Person
) as _proj