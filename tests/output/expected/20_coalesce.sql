SELECT
   COALESCE(__p_nickname, __p_name) AS displayName
FROM (
  SELECT
     id AS __p_id
    ,name AS __p_name
    ,nickname AS __p_nickname
  FROM
    `graph`.`Person`
) AS _proj
