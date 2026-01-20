select
 _gsql2rsql_p_name as name
 ,case when (_gsql2rsql_p_age) < (18) then 'minor' when (_gsql2rsql_p_age) >= (65) then 'senior' else 'adult' end as ageGroup
from (
 select
 id as _gsql2rsql_p_id
 ,name as _gsql2rsql_p_name
 ,age as _gsql2rsql_p_age
 from
 graph.Person
) as _proj