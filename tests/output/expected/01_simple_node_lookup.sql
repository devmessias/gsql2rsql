select
 _gsql2rsql_p_id as p
 ,_gsql2rsql_p_name as _gsql2rsql_p_name
from (
 select
 id as _gsql2rsql_p_id
 ,name as _gsql2rsql_p_name
 from
 Person
) as _proj