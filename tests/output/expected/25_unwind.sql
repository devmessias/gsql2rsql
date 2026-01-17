select
 __a_id as id
 ,tag as tag
from (
 select
 _unwind_source.*
 ,tag
 from (
 select
 id as __a_id
 from
 graph.Account
 ) as _unwind_source,
 EXPLODE(__a_tags) as _exploded(tag)
) as _proj