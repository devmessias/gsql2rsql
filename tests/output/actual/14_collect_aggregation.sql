select
 __c_name as city
 ,COLLECT_LIST(__p_name) as residents
from (
 select
 _left.__c_id as __c_id
 ,_left.__c_name as __c_name
 ,_left.___anon1_source_id as ___anon1_source_id
 ,_left.___anon1_target_id as ___anon1_target_id
 ,_right.__p_id as __p_id
 ,_right.__p_name as __p_name
 from (
 select
 _left.__c_id as __c_id
 ,_left.__c_name as __c_name
 ,_right.___anon1_source_id as ___anon1_source_id
 ,_right.___anon1_target_id as ___anon1_target_id
 from (
 select
 id as __c_id
 ,name as __c_name
 from
 graph.City
 ) as _left
 inner join (
 select
 source_id as ___anon1_source_id
 ,target_id as ___anon1_target_id
 from
 graph.LivesIn
 ) as _right on
 _left.__c_id = _right.___anon1_target_id
 ) as _left
 inner join (
 select
 id as __p_id
 ,name as __p_name
 from
 graph.Person
 ) as _right on
 _right.__p_id = _left.___anon1_source_id
) as _proj
group by __c_name