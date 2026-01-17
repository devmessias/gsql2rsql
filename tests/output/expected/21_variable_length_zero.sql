with recursive
 paths_1 as (
 -- Base case: Zero-length paths (depth = 0)
 select
 n.id as start_node,
 n.id as end_node,
 0 as depth,
 array(n.id) as path,
 array() as visited
 from graph.Person n
 union all
 -- Base case: direct edges (depth = 1)
 select
 e.source_id as start_node,
 e.target_id as end_node,
 1 as depth,
 array(e.source_id, e.target_id) as path,
 array(e.source_id) as visited
 from graph.Knows e
 union all
 -- recursive case: extend paths
 select
 p.start_node,
 e.target_id as end_node,
 p.depth + 1 as depth,
 CONCAT(p.path, array(e.target_id)) as path,
 CONCAT(p.visited, array(e.source_id)) as visited
 from paths_1 p
 join graph.Knows e
 on p.end_node = e.source_id
 where p.depth < 2
 and not array_contains(p.visited, e.target_id)
 )
select distinct
 __f_name as name
from (
 select
 n.id as __f_id
 ,n.name as __f_name
 ,n.age as __f_age
 ,p.start_node as __p_id
 ,p.start_node
 ,p.end_node
 ,p.depth
 ,p.path
 from paths_1 p
 join graph.Person n
 on n.id = p.end_node
 where p.depth >= 0 and p.depth <= 2
) as _proj