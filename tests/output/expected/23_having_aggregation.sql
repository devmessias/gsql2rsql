select
 _gsql2rsql_a_id as id
 ,transferCount as transferCount
 ,totalAmount as totalAmount
from (
 select
 _gsql2rsql_a_id as a
 ,count(_gsql2rsql_b_id) as transferCount
 ,sum(_gsql2rsql_t_amount) as totalAmount
 ,_gsql2rsql_a_name as _gsql2rsql_a_name
 ,_gsql2rsql_a_risk_score as _gsql2rsql_a_risk_score
 from (
 select
 _left._gsql2rsql_a_id as _gsql2rsql_a_id
 ,_left._gsql2rsql_a_name as _gsql2rsql_a_name
 ,_left._gsql2rsql_a_risk_score as _gsql2rsql_a_risk_score
 ,_left._gsql2rsql_t_source_id as _gsql2rsql_t_source_id
 ,_left._gsql2rsql_t_target_id as _gsql2rsql_t_target_id
 ,_left._gsql2rsql_t_amount as _gsql2rsql_t_amount
 ,_right._gsql2rsql_b_id as _gsql2rsql_b_id
 from (
 select
 _left._gsql2rsql_a_id as _gsql2rsql_a_id
 ,_left._gsql2rsql_a_name as _gsql2rsql_a_name
 ,_left._gsql2rsql_a_risk_score as _gsql2rsql_a_risk_score
 ,_right._gsql2rsql_t_source_id as _gsql2rsql_t_source_id
 ,_right._gsql2rsql_t_target_id as _gsql2rsql_t_target_id
 ,_right._gsql2rsql_t_amount as _gsql2rsql_t_amount
 from (
 select
 id as _gsql2rsql_a_id
 ,name as _gsql2rsql_a_name
 ,risk_score as _gsql2rsql_a_risk_score
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
 group by _gsql2rsql_a_id, _gsql2rsql_a_name, _gsql2rsql_a_risk_score
 having ((transferCount) > (100)) and ((totalAmount) > (1000000))
) as _proj