select
 _gsql2rsql_a_id as id
 ,_gsql2rsql_a_name as name
from (
 select *
 from (
 select
 id as _gsql2rsql_a_id
 ,name as _gsql2rsql_a_name
 from
 graph.Account
 ) as _filter
 where array_contains(:watchlist, _gsql2rsql_a_id)
) as _proj