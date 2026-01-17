select
 __a_id as id
from (
 select
 a as a
 ,COLLECT_LIST(__t_amount) as amounts
 from (
 select
 _left.__a_id as __a_id
 ,_left.__a_name as __a_name
 ,_left.__a_verified as __a_verified
 ,_left.__t_source_id as __t_source_id
 ,_left.__t_target_id as __t_target_id
 ,_left.__t_amount as __t_amount
 ,_left.__t_flagged as __t_flagged
 ,_left.__t_timestamp as __t_timestamp
 ,_right.__b_id as __b_id
 ,_right.__b_name as __b_name
 ,_right.__b_verified as __b_verified
 from (
 select
 _left.__a_id as __a_id
 ,_left.__a_name as __a_name
 ,_left.__a_verified as __a_verified
 ,_right.__t_source_id as __t_source_id
 ,_right.__t_target_id as __t_target_id
 ,_right.__t_amount as __t_amount
 ,_right.__t_flagged as __t_flagged
 ,_right.__t_timestamp as __t_timestamp
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
 group by a
 having FORALL(amounts, x -> (x) > (1000))
) as _proj