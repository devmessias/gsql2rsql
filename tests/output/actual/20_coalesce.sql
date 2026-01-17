select
 coalesce(__p_nickname, __p_name) as displayName
from (
 select
 id as __p_id
 ,name as __p_name
 ,nickname as __p_nickname
 ,alias as __p_alias
 ,age as __p_age
 ,city as __p_city
 from
 graph.Person
) as _proj