select
 _gsql2rsql_a_id as id
 ,tag as tag
from (
 select
 _unwind_source.*
 ,tag
 from (
 select
 id as _gsql2rsql_a_id
 ,tags as _gsql2rsql_a_tags
 from
 graph.Account
 ) as _unwind_source,
 EXPLODE(coalesce(_gsql2rsql_a_tags, ('no_tags'))) as _exploded(tag)
) as _proj