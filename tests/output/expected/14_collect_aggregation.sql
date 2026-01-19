select
 _gsql2rsql_c_name as city
 ,COLLECT_LIST(_gsql2rsql_p_name) as residents
from (
 select
 _left._gsql2rsql_c_id as _gsql2rsql_c_id
 ,_left._gsql2rsql_c_name as _gsql2rsql_c_name
 ,_left._gsql2rsql__anon1_source_id as _gsql2rsql__anon1_source_id
 ,_left._gsql2rsql__anon1_target_id as _gsql2rsql__anon1_target_id
 ,_right._gsql2rsql_p_id as _gsql2rsql_p_id
 ,_right._gsql2rsql_p_name as _gsql2rsql_p_name
 from (
 select
 _left._gsql2rsql_c_id as _gsql2rsql_c_id
 ,_left._gsql2rsql_c_name as _gsql2rsql_c_name
 ,_right._gsql2rsql__anon1_source_id as _gsql2rsql__anon1_source_id
 ,_right._gsql2rsql__anon1_target_id as _gsql2rsql__anon1_target_id
 from (
 select
 id as _gsql2rsql_c_id
 ,name as _gsql2rsql_c_name
 from
 graph.City
 ) as _left
 inner join (
 select
 source_id as _gsql2rsql__anon1_source_id
 ,target_id as _gsql2rsql__anon1_target_id
 from
 graph.LivesIn
 ) as _right on
 _left._gsql2rsql_c_id = _right._gsql2rsql__anon1_target_id
 ) as _left
 inner join (
 select
 id as _gsql2rsql_p_id
 ,name as _gsql2rsql_p_name
 from
 graph.Person
 ) as _right on
 _right._gsql2rsql_p_id = _left._gsql2rsql__anon1_source_id
) as _proj
group by _gsql2rsql_c_name