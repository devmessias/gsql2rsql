select
 _gsql2rsql_a_id as id
 ,tag as tag
 ,txId as txId
from (
 select
 _unwind_source.*
 ,txId
 from (
 select
 _unwind_source.*
 ,tag
 from (
 select
 id as _gsql2rsql_a_id
 ,tags as _gsql2rsql_a_tags
 ,transactionIds as _gsql2rsql_a_transactionIds
 from
 graph.Account
 ) as _unwind_source,
 EXPLODE(_gsql2rsql_a_tags) as _exploded(tag)
 ) as _unwind_source,
 EXPLODE(_gsql2rsql_a_transactionIds) as _exploded(txId)
) as _proj