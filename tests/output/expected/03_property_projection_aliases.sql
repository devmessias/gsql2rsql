select
 _gsql2rsql_p_name as personName
 ,_gsql2rsql_p_id as personId
from (
 select
 id as _gsql2rsql_p_id
 ,name as _gsql2rsql_p_name
 from
 Person
) as _proj