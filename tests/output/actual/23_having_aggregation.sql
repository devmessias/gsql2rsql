select
 __a_id as id
 ,transferCount as transferCount
 ,totalAmount as totalAmount
from (
 select
 a as a
 ,count(b) as transferCount
 ,sum(__t_amount) as totalAmount
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
 group by a
 having ((transferCount) > (100)) and ((totalAmount) > (1000000))
) as _proj