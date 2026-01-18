select
 a as id
from (
 select
 __a_id as a
 ,COLLECT_LIST(__t_amount) as amounts
 from (
 select
 _left.__a_id as __a_id
 ,_left.__t_source_id as __t_source_id
 ,_left.__t_target_id as __t_target_id
 ,_left.__t_amount as __t_amount
 ,_right.__b_id as __b_id
 from (
 select
 _left.__a_id as __a_id
 ,_right.__t_source_id as __t_source_id
 ,_right.__t_target_id as __t_target_id
 ,_right.__t_amount as __t_amount
 from (
 select
 id as __a_id
 from
 graph.Account
 ) as _left
 inner join (
 select
 source_id as __t_source_id
 ,target_id as __t_target_id
 ,amount as __t_amount
 from
 graph.Transfer
 ) as _right on
 _left.__a_id = _right.__t_source_id
 ) as _left
 inner join (
 select
 id as __b_id
 from
 graph.Account
 ) as _right on
 _right.__b_id = _left.__t_target_id
 ) as _proj
 group by __a_id
 having FORALL(amounts, x -> (x) > (1000))
) as _proj