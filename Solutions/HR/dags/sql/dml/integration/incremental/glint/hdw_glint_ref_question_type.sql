BEGIN
DECLARE
  DUP_COUNT INT64;
BEGIN TRANSACTION;
  INSERT INTO
    {{ params.param_hr_core_dataset_name }}.ref_question_type (question_type_code,
      question_type_desc,
      source_system_code,
      dw_last_update_date_time)
  SELECT
    LPAD(CAST(ROW_NUMBER() OVER (ORDER BY a.question_type) + (
        SELECT
          CAST(COALESCE(MAX(question_type_code), '0') AS INT64)
        FROM
          {{ params.param_hr_base_views_dataset_name }}.ref_question_type
        WHERE
          UPPER(question_type_code) <> 'UNK' ) AS STRING), 10),
    a.question_type,
    'G' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
  FROM (
    SELECT
      glint_question.question_type
    FROM
      {{ params.param_hr_stage_dataset_name }}.glint_question
    GROUP BY
      1 ) AS a
  WHERE
    TRIM(a.question_type) NOT IN(
    SELECT
      DISTINCT TRIM(question_type_desc)
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_question_type AS ref_question_type_0
    WHERE
      Trim(Source_System_Code) = 'G' ) ; /* Test Unique Primary Index constraint set in Terdata */
  SET
    DUP_COUNT = (
    SELECT
      COUNT(*)
    FROM (
      SELECT
        Question_Type_Code
      FROM
        {{ params.param_hr_core_dataset_name }}.ref_question_type
      GROUP BY
        Question_Type_Code
      HAVING
        COUNT(*) > 1 ) );
  IF
    DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION; RAISE
  USING
    MESSAGE = 'Duplicates not allowed in table:{{ params.param_hr_core_dataset_name }}.ref_question_type';
    ELSE
  COMMIT TRANSACTION;
  END IF
    ;
END
  ;