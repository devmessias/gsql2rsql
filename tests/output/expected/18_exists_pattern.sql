select
 _gsql2rsql_p_name as name
from (
 select *
 from (
 select
 id as _gsql2rsql_p_id
 ,name as _gsql2rsql_p_name
 from
 graph.Person
 ) as _filter
 where exists (select 1 from graph.ActedIn _exists_rel join graph.Movie _exists_target on _exists_rel.target_id = _exists_target.id where _exists_rel.source_id = _gsql2rsql_p_id)
) as _proj