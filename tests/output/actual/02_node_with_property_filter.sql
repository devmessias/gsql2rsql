select
 _gsql2rsql_p_id as p
from (
 select *
 from (
 select
 id as _gsql2rsql_p_id
 ,name as _gsql2rsql_p_name
 from
 Person
 ) as _filter
 where (_gsql2rsql_p_name) = ('Alice')
) as _proj