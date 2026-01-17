SELECT
   __p_name AS name
FROM (
  SELECT *
  FROM (
    SELECT
       id AS __p_id
      ,name AS __p_name
    FROM
      `graph`.`Person`
  ) AS _filter
  WHERE EXISTS (SELECT 1 FROM `graph`.`ActedIn` _exists_rel JOIN `graph`.`Movie` _exists_target ON _exists_rel.target_id = _exists_target.id WHERE _exists_rel.source_id = __p_id)
) AS _proj
