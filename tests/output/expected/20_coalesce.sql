select
 coalesce(_gsql2rsql_p_nickname, _gsql2rsql_p_name) as displayName
from (
 select
 id as _gsql2rsql_p_id
 ,name as _gsql2rsql_p_name
 ,nickname as _gsql2rsql_p_nickname
 from
 graph.Person
) as _proj