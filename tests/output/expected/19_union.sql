select
 _gsql2rsql_p_name as name
from (
 select
 id as _gsql2rsql_p_id
 ,name as _gsql2rsql_p_name
 from
 graph.Person
) as _proj
union
select
 _gsql2rsql_c_name as name
from (
 select
 id as _gsql2rsql_c_id
 ,name as _gsql2rsql_c_name
 from
 graph.City
) as _proj