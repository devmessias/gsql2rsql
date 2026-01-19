select
 _gsql2rsql_a_id as id
 ,idx as idx
from (
 select
 _unwind_source.*
 ,idx
 from (
 select
 id as _gsql2rsql_a_id
 from
 graph.Account
 ) as _unwind_source,
 EXPLODE(SEQUENCE(0, 5)) as _exploded(idx)
) as _proj