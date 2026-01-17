SELECT 
   __a_id AS id
  ,tag AS tag
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
  EXPLODE(COALESCE(__a_tags, ('no_tags'))) AS _exploded(tag)
) AS _proj
