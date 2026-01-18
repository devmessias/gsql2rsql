select
 __c_name as city
 ,count(p) as population
from (
 select
 _left.__p_id as __p_id
 ,_left.___anon1_source_id as ___anon1_source_id
 ,_left.___anon1_target_id as ___anon1_target_id
 ,_right.__c_id as __c_id
 ,_right.__c_name as __c_name
 from (
 select
 _left.__p_id as __p_id
 ,_right.___anon1_source_id as ___anon1_source_id
 ,_right.___anon1_target_id as ___anon1_target_id
 from (
 select
 id as __p_id
 from
 graph.Person
 ) as _left
 inner join (
 select
 source_id as ___anon1_source_id
 ,target_id as ___anon1_target_id
 from
 graph.LivesIn
 ) as _right on
 _left.__p_id = _right.___anon1_source_id
 ) as _left
 inner join (
 select
 id as __c_id
 ,name as __c_name
 from
 graph.City
 ) as _right on
 _right.__c_id = _left.___anon1_target_id
) as _proj
group by __c_name