select
 __p_id as p
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