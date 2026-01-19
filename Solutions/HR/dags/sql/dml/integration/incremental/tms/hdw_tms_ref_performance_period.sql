BEGIN
DECLARE DUP_COUNT INT64;

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_performance_period_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_performance_period_wrk (review_period_id, review_period_desc, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              COALESCE(max(review_period_id), CAST(0 as int64))
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_performance_period
        ) + CAST(row_number() OVER (ORDER BY y.review_period_desc) as int64) AS review_period_id,
        y.review_period_desc,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              stg.review_period_desc AS review_period_desc
            FROM
              (
                SELECT DISTINCT
                    TRIM(competency_ratings_report.review_period) AS review_period_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.competency_ratings_report
                  WHERE TRIM(competency_ratings_report.review_period) IS NOT NULL
                   AND TRIM(competency_ratings_report.review_period) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(performance_ratings_report.rev_period) AS review_period_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.performance_ratings_report
                  WHERE TRIM(performance_ratings_report.rev_period) IS NOT NULL
                   AND TRIM(performance_ratings_report.rev_period) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(employee_perf_goals.review_period) AS review_period_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_perf_goals
                  WHERE TRIM(employee_perf_goals.review_period) IS NOT NULL
                   AND TRIM(employee_perf_goals.review_period) <> ''
              ) AS stg
              LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_performance_period_wrk AS tgt 
              ON UPPER(TRIM(COALESCE(stg.review_period_desc,''))) = UPPER(TRIM(COALESCE(tgt.review_period_desc,'')))
            WHERE tgt.review_period_desc IS NULL
        ) AS y
  ;

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_performance_period (review_period_id, review_period_desc, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(row_number() OVER (ORDER BY wrk.review_period_id) as INT64) + (
          SELECT
              COALESCE(max(review_period_id), CAST(0 as INT64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_performance_period
        ) AS review_period_id,
        TRIM(wrk.review_period_desc),
        wrk.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_performance_period_wrk AS wrk
      WHERE UPPER(TRIM(COALESCE(wrk.review_period_desc,''))) NOT IN(
        SELECT
            UPPER(TRIM(COALESCE(review_period_desc,'')))
          FROM
           {{ params.param_hr_core_dataset_name }}.ref_performance_period
      )
 ;
    SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT review_period_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_performance_period
      GROUP BY review_period_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
    ROLLBACK TRANSACTION;
    RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.ref_performance_period ');
  ELSE
    COMMIT TRANSACTION;
  END IF;

END;
