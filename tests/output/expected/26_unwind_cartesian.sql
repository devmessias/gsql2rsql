SELECT 
   __a_id AS id
  ,tag AS tag
  ,txId AS txId
FROM (
  SELECT
     _unwind_source.*
    ,txId
  FROM (
    SELECT
       _unwind_source.*
      ,tag
    FROM (
      SELECT
         id AS __a_id
      FROM
        `graph`.`Account`
    ) AS _unwind_source,
    EXPLODE(__a_tags) AS _exploded(tag)
  ) AS _unwind_source,
  EXPLODE(__a_transactionIds) AS _exploded(txId)
) AS _proj
