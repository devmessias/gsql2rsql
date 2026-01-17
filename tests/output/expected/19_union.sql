SELECT
   __p_name AS name
FROM (
  SELECT
     id AS __p_id
    ,name AS __p_name
  FROM
    `graph`.`Person`
) AS _proj
UNION
SELECT
   __c_name AS name
FROM (
  SELECT
     id AS __c_id
    ,name AS __c_name
  FROM
    `graph`.`City`
) AS _proj
