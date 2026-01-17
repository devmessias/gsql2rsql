select
 __p_name as personName
 ,__p_id as personId
from (
 select
 id as __p_id
 ,name as __p_name
 from
 dbo.Person
) as _proj
