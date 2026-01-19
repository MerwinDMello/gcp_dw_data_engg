BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION; 
    INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_performance_category 
    (performance_category_id, performance_category_desc, source_system_code, dw_last_update_date_time) 
    SELECT
      (
      SELECT
        COALESCE(MAX(performance_category_id), CAST(0 AS INT64))
      FROM
        {{ params.param_hr_base_views_dataset_name }}.ref_performance_category ) 
      + CAST(ROW_NUMBER() OVER (ORDER BY UPPER(TRIM(stg.goal_category))) AS INT64) AS performance_category_id,
      stg.goal_category AS performance_category_desc,
      'M' AS source_system_code,
      DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
    FROM (
      SELECT
        DISTINCT goal_category AS goal_category
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_perf_goals
      WHERE
        goal_category IS NOT NULL ) AS stg
    LEFT OUTER JOIN
      {{ params.param_hr_base_views_dataset_name }}.ref_performance_category AS tgt
    ON
      UPPER(TRIM(tgt.performance_category_desc)) = UPPER(TRIM(stg.goal_category))
    WHERE
      tgt.performance_category_id IS NULL;

/* Test Primary Index constraint set in Teradata */
      SET DUP_COUNT = ( 
          select count(*)
          from (
          select
              performance_category_desc
          from {{ params.param_hr_core_dataset_name }}.ref_performance_category
          group by performance_category_desc
          having count(*) > 1
          )
      );
      IF DUP_COUNT <> 0 THEN
        ROLLBACK TRANSACTION;
        RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_performance_category');
      ELSE
        COMMIT TRANSACTION;
      END IF;
  END;

