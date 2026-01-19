select
 _gsql2rsql_a_id as id
 ,count(tag) as tagCount
from (
 select
 _unwind_source.*
 ,tag
 from (
 select
 id as _gsql2rsql_a_id
 from
 graph.Account
 ) as _unwind_source,
 EXPLODE(_gsql2rsql_a_tags) as _exploded(tag)
) as _proj
group by _gsql2rsql_a_id