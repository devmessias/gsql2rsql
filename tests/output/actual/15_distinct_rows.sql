select distinct
 __f_name as name
from (
 select
 _left.__p_id as __p_id
 ,_left.___anon1_source_id as ___anon1_source_id
 ,_left.___anon1_target_id as ___anon1_target_id
 ,_right.__f_id as __f_id
 ,_right.__f_name as __f_name
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
 graph.Knows
 ) as _right on
 _left.__p_id = _right.___anon1_source_id
 ) as _left
 inner join (
 select
 id as __f_id
 ,name as __f_name
 from
 graph.Person
 ) as _right on
 _right.__f_id = _left.___anon1_target_id
) as _proj