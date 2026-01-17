SELECT
   __a_id AS id
  ,transferCount AS transferCount
  ,totalAmount AS totalAmount
FROM (
  SELECT
     a AS a
    ,COUNT(b) AS transferCount
    ,SUM(__t_amount) AS totalAmount
  FROM (
    SELECT
       _left.__a_id AS __a_id
      ,_left.__a_name AS __a_name
      ,_left.__a_risk_score AS __a_risk_score
      ,_left.__t_source_id AS __t_source_id
      ,_left.__t_target_id AS __t_target_id
      ,_left.__t_amount AS __t_amount
      ,_left.__t_timestamp AS __t_timestamp
      ,_right.__b_id AS __b_id
      ,_right.__b_name AS __b_name
      ,_right.__b_risk_score AS __b_risk_score
    FROM (
      SELECT
         _left.__a_id AS __a_id
        ,_left.__a_name AS __a_name
        ,_left.__a_risk_score AS __a_risk_score
        ,_right.__t_source_id AS __t_source_id
        ,_right.__t_target_id AS __t_target_id
        ,_right.__t_amount AS __t_amount
        ,_right.__t_timestamp AS __t_timestamp
      FROM (
        SELECT
           id AS __a_id
        FROM
          `graph`.`Account`
      ) AS _left
      INNER JOIN (
        SELECT
           source_id AS __t_source_id
          ,target_id AS __t_target_id
          ,amount AS __t_amount
        FROM
          `graph`.`Transfer`
      ) AS _right ON
        _left.__a_id = _right.__t_source_id
    ) AS _left
    INNER JOIN (
      SELECT
         id AS __b_id
      FROM
        `graph`.`Account`
    ) AS _right ON
      _right.__b_id = _left.__t_target_id
  ) AS _proj
  GROUP BY a
  HAVING ((transferCount) > (100)) AND ((totalAmount) > (1000000))
) AS _proj
