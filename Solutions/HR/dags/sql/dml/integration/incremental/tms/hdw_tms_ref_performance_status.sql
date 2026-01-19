BEGIN
DECLARE DUP_COUNT INT64;
  
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_performance_status_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_performance_status_wrk (performance_status_id, performance_status_desc, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              COALESCE(max(performance_status_id), CAST(0 as INT64))
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_performance_status
        ) + CAST(row_number() OVER (ORDER BY y.performance_status_desc) as INT64) AS performance_status_id,
        y.performance_status_desc,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              TRIM(stg.performance_status_desc) AS performance_status_desc
            FROM
              (
                SELECT DISTINCT
                    TRIM(activities_report.status) AS performance_status_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.activities_report
                  WHERE TRIM(activities_report.status) IS NOT NULL
                   AND TRIM(activities_report.status) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(competency_ratings_report.evaluation_workflow_state) AS performance_status_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.competency_ratings_report
                  WHERE TRIM(competency_ratings_report.evaluation_workflow_state) IS NOT NULL
                   AND TRIM(competency_ratings_report.evaluation_workflow_state) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(employee_perf_goals.goal_status) AS performance_status_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_perf_goals
                  WHERE TRIM(employee_perf_goals.goal_status) IS NOT NULL
                   AND TRIM(employee_perf_goals.goal_status) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(employee_perf_goals_0.goal_progress) AS performance_status_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_perf_goals AS employee_perf_goals_0
                  WHERE TRIM(employee_perf_goals_0.goal_progress) IS NOT NULL
                   AND TRIM(employee_perf_goals_0.goal_progress) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(performance_ratings_report.eval_workflow_state) AS performance_status_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.performance_ratings_report
                  WHERE TRIM(performance_ratings_report.eval_workflow_state) IS NOT NULL
                   AND TRIM(performance_ratings_report.eval_workflow_state) <> ''
              ) AS stg
              LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_performance_status_wrk AS tgt 
              ON UPPER(TRIM(COALESCE(stg.performance_status_desc,''))) = UPPER(TRIM(COALESCE(tgt.performance_status_desc,'')))
            WHERE tgt.performance_status_desc IS NULL
        ) AS y
  ;

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_performance_status (performance_status_id, performance_status_desc, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(row_number() OVER (ORDER BY wrk.performance_status_id) as int64) + (
          SELECT
              COALESCE(max(performance_status_id), CAST(0 as int64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_performance_status
        ) AS performance_status_id,
        TRIM(wrk.performance_status_desc),
        wrk.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_performance_status_wrk AS wrk
      WHERE UPPER(COALESCE(REGEXP_REPLACE(wrk.performance_status_desc, r'([^\x20-\x7E]+)', ''),''))  NOT IN(
        SELECT
            UPPER(COALESCE(REGEXP_REPLACE(performance_status_desc, r'([^\x20-\x7E]+)', ''),''))
          FROM
            {{ params.param_hr_core_dataset_name }}.ref_performance_status
      )
      
  ;
    SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT performance_status_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_performance_status
      GROUP BY performance_status_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
    ROLLBACK TRANSACTION;
    RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.ref_performance_status');
  ELSE
    COMMIT TRANSACTION;
  END IF;

END;
