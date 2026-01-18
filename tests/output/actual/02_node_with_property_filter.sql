select
 __p_id as p
 ,__p_id as __p_id
 ,__p_name as __p_name
from (
 select *
 from (
 select
 id as __p_id
 ,name as __p_name
 from
 Person
 ) as _filter
 where (__p_name) = ('Alice')
) as _proj