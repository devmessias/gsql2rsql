SELECT
   __a_id AS id
  ,__a_name AS name
FROM (
  SELECT *
  FROM (
    SELECT
       id AS __a_id
      ,name AS __a_name
    FROM
      `graph`.`Account`
  ) AS _filter
  WHERE ARRAY_CONTAINS(:watchlist, __a_id)
) AS _proj
