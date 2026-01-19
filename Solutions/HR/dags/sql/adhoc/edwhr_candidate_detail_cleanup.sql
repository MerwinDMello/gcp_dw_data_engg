-- Delete duplicate records

DELETE
FROM
  edwhr.candidate_detail
WHERE
  STRUCT (candidate_sid,
    COALESCE(element_detail_entity_text,''),
    COALESCE(element_detail_type_text,''),
    element_detail_seq,
    COALESCE(element_detail_id,0),
    COALESCE(TRIM(element_detail_value_text),''),
    COALESCE(source_system_code,''),
    valid_from_date,
    valid_to_date) IN (
  SELECT
    STRUCT (candidate_sid,
      COALESCE(element_detail_entity_text,''),
      COALESCE(element_detail_type_text,''),
      element_detail_seq,
      COALESCE(element_detail_id,0),
      COALESCE(TRIM(element_detail_value_text),''),
      COALESCE(source_system_code,''),
      valid_from_date,
      valid_to_date)
  FROM (
    WITH
      duplicates AS (
      SELECT
        x.*
      FROM
        edwhr.candidate_detail x
      INNER JOIN (
        SELECT
          candidate_sid,
          COALESCE(element_detail_entity_text,'') AS element_detail_entity_text,
          COALESCE(element_detail_type_text,'') AS element_detail_type_text,
          element_detail_seq,
          COALESCE(element_detail_id,0) AS element_detail_id,
          COALESCE(TRIM(element_detail_value_text),'') AS element_detail_value_text,
          COALESCE(source_system_code,'') AS source_system_code,
          MIN(valid_from_date) AS valid_from_date
        FROM
          edwhr.candidate_detail
        WHERE
          DATE(valid_to_date) = '9999-12-31' --and candidate_sid = 2448079
        GROUP BY
          candidate_sid,
          element_detail_entity_text,
          element_detail_type_text,
          element_detail_seq,
          element_detail_id,
          element_detail_value_text,
          source_system_code
        HAVING
          (COUNT(*)) > 1 ) y
      ON
        DATE(x.valid_from_date) <> DATE(y.valid_from_date)
        AND DATE(x.valid_to_date) = '9999-12-31'
        AND STRUCT(x.candidate_sid,
          COALESCE(x.element_detail_entity_text,''),
          COALESCE(x.element_detail_type_text,''),
          x.element_detail_seq,
          COALESCE(x.element_detail_id,0),
          COALESCE(TRIM(x.element_detail_value_text),''),
          COALESCE(x.source_system_code,'')) = STRUCT ( y.candidate_sid,
          y.element_detail_entity_text,
          y.element_detail_type_text,
          y.element_detail_seq,
          y.element_detail_id,
          TRIM(y.element_detail_value_text),
          y.source_system_code) )
    SELECT
      *
    FROM
      duplicates ));

-- Reverse the incorrect updates

UPDATE
  edwhr.candidate_detail x
SET
  x.valid_to_date = y.valid_to_date,
  x.dw_last_update_date_time = x.valid_from_date
FROM (
  SELECT
    candidate_sid,
    COALESCE(element_detail_entity_text,'') AS element_detail_entity_text,
    COALESCE(element_detail_type_text,'') AS element_detail_type_text,
    element_detail_seq,
    COALESCE(element_detail_id,0) AS element_detail_id,
    COALESCE(TRIM(element_detail_value_text),'') AS element_detail_value_text,
    COALESCE(source_system_code,'') AS source_system_code,
    valid_from_date,
    valid_to_date
  FROM
    edwhr.candidate_detail
  WHERE
    source_system_code = 'T'
    AND DATE(valid_to_date) = '9999-12-31'
    AND element_detail_value_text IS NULL ) y
WHERE
  DATE(x.valid_to_date) = DATE(y.valid_from_date)
  AND STRUCT(x.candidate_sid,
    COALESCE(x.element_detail_entity_text,''),
    COALESCE(x.element_detail_type_text,''),
    x.element_detail_seq,
    COALESCE(x.element_detail_id,0),
    COALESCE(x.source_system_code,'')) = STRUCT ( y.candidate_sid,
    COALESCE(y.element_detail_entity_text,''),
    COALESCE(y.element_detail_type_text,''),
    y.element_detail_seq,
    COALESCE(y.element_detail_id,0),
    COALESCE(y.source_system_code,'') )
  AND x.source_system_code = 'T'
  AND DATE(x.valid_to_date) <> '9999-12-31';

-- Delete incorrect null records

DELETE
FROM
  edwhr.candidate_detail
WHERE
  STRUCT (candidate_sid,
    COALESCE(element_detail_entity_text,''),
    COALESCE(element_detail_type_text,''),
    element_detail_seq,
    COALESCE(element_detail_id,0),
    COALESCE(TRIM(element_detail_value_text),''),
    COALESCE(source_system_code,''),
    valid_from_date,
    valid_to_date) IN (
  SELECT
    STRUCT (y.candidate_sid,
      COALESCE(y.element_detail_entity_text,''),
      COALESCE(y.element_detail_type_text,''),
      y.element_detail_seq,
      COALESCE(y.element_detail_id,0),
      COALESCE(TRIM(y.element_detail_value_text),''),
      COALESCE(y.source_system_code,''),
      y.valid_from_date,
      y.valid_to_date)
  FROM
    edwhr.candidate_detail x
  INNER JOIN (
    SELECT
      candidate_sid,
      COALESCE(element_detail_entity_text,'') AS element_detail_entity_text,
      COALESCE(element_detail_type_text,'') AS element_detail_type_text,
      element_detail_seq,
      COALESCE(element_detail_id,0) AS element_detail_id,
      COALESCE(TRIM(element_detail_value_text),'') AS element_detail_value_text,
      COALESCE(source_system_code,'') AS source_system_code,
      valid_from_date,
      valid_to_date
    FROM
      edwhr.candidate_detail
    WHERE
      source_system_code = 'T'
      AND DATE(valid_to_date) = '9999-12-31'
      AND element_detail_value_text IS NULL ) y
  ON
    STRUCT(x.candidate_sid,
      COALESCE(x.element_detail_entity_text,''),
      COALESCE(x.element_detail_type_text,''),
      x.element_detail_seq,
      COALESCE(x.element_detail_id,0),
      COALESCE(x.source_system_code,'')) = STRUCT ( y.candidate_sid,
      COALESCE(y.element_detail_entity_text,''),
      COALESCE(y.element_detail_type_text,''),
      y.element_detail_seq,
      COALESCE(y.element_detail_id,0),
      COALESCE(y.source_system_code,'') )
    AND x.source_system_code = 'T'
    AND DATE(x.valid_from_date) < '2023-05-05'
    AND DATE(y.valid_from_date) BETWEEN '2023-05-05'
    AND '2023-05-07'
    AND DATE(x.valid_to_date) = '9999-12-31');