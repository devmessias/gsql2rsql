SELECT 
   __a_id AS id
  ,idx AS idx
FROM (
  SELECT
     _unwind_source.*
    ,idx
  FROM (
    SELECT
       id AS __a_id
    FROM
      `graph`.`Account`
  ) AS _unwind_source,
  EXPLODE(SEQUENCE(0, 5)) AS _exploded(idx)
) AS _proj
