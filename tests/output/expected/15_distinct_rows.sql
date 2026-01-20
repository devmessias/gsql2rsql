select distinct
 _gsql2rsql_f_name as name
from (
 select
 _left._gsql2rsql_p_id as _gsql2rsql_p_id
 ,_left._gsql2rsql__anon1_source_id as _gsql2rsql__anon1_source_id
 ,_left._gsql2rsql__anon1_target_id as _gsql2rsql__anon1_target_id
 ,_right._gsql2rsql_f_id as _gsql2rsql_f_id
 ,_right._gsql2rsql_f_name as _gsql2rsql_f_name
 from (
 select
 _left._gsql2rsql_p_id as _gsql2rsql_p_id
 ,_right._gsql2rsql__anon1_source_id as _gsql2rsql__anon1_source_id
 ,_right._gsql2rsql__anon1_target_id as _gsql2rsql__anon1_target_id
 from (
 select
 id as _gsql2rsql_p_id
 from
 graph.Person
 ) as _left
 inner join (
 select
 source_id as _gsql2rsql__anon1_source_id
 ,target_id as _gsql2rsql__anon1_target_id
 from
 graph.Knows
 ) as _right on
 _left._gsql2rsql_p_id = _right._gsql2rsql__anon1_source_id
 ) as _left
 inner join (
 select
 id as _gsql2rsql_f_id
 ,name as _gsql2rsql_f_name
 from
 graph.Person
 ) as _right on
 _right._gsql2rsql_f_id = _left._gsql2rsql__anon1_target_id
) as _proj