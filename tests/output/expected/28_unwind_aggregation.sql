SELECT 
   __a_id AS id
  ,COUNT(tag) AS tagCount
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
) AS _proj
GROUP BY __a_id
