select
 a as id
from (
 select
 _gsql2rsql_a_id as a
 ,COLLECT_LIST(_gsql2rsql_t_amount) as amounts
 from (
 select
 _left._gsql2rsql_a_id as _gsql2rsql_a_id
 ,_left._gsql2rsql_t_source_id as _gsql2rsql_t_source_id
 ,_left._gsql2rsql_t_target_id as _gsql2rsql_t_target_id
 ,_left._gsql2rsql_t_amount as _gsql2rsql_t_amount
 ,_right._gsql2rsql_b_id as _gsql2rsql_b_id
 from (
 select
 _left._gsql2rsql_a_id as _gsql2rsql_a_id
 ,_right._gsql2rsql_t_source_id as _gsql2rsql_t_source_id
 ,_right._gsql2rsql_t_target_id as _gsql2rsql_t_target_id
 ,_right._gsql2rsql_t_amount as _gsql2rsql_t_amount
 from (
 select
 id as _gsql2rsql_a_id
 from
 graph.Account
 ) as _left
 inner join (
 select
 source_id as _gsql2rsql_t_source_id
 ,target_id as _gsql2rsql_t_target_id
 ,amount as _gsql2rsql_t_amount
 from
 graph.Transfer
 ) as _right on
 _left._gsql2rsql_a_id = _right._gsql2rsql_t_source_id
 ) as _left
 inner join (
 select
 id as _gsql2rsql_b_id
 from
 graph.Account
 ) as _right on
 _right._gsql2rsql_b_id = _left._gsql2rsql_t_target_id
 ) as _proj
 group by _gsql2rsql_a_id
 having FORALL(amounts, x -> (x) > (1000))
) as _proj