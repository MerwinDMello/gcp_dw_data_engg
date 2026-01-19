BEGIN
  DECLARE DUP_COUNT INT64;
/* Load Work Table with working Data */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_performance_plan_wrk;
  BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_performance_plan_wrk 
    SELECT DISTINCT
        coalesce(a.performance_plan_id, 'U'),
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_time
      FROM
        (
          SELECT DISTINCT
              trim(employee_perf_goals.plan_name) AS performance_plan_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee_perf_goals
          UNION ALL
          SELECT DISTINCT
              trim(performance_ratings_report.plan_name) AS performance_plan_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.performance_ratings_report
          UNION ALL
          SELECT DISTINCT
              trim(competency_ratings_report.plan_name) AS performance_plan_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.competency_ratings_report
        ) AS a
  ;
   SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Performance_Plan_Desc
        from {{ params.param_hr_stage_dataset_name }}.ref_performance_plan_wrk
        group by Performance_Plan_Desc
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_performance_plan_wrk');
    ELSE
      COMMIT TRANSACTION;
    END IF;
  BEGIN TRANSACTION; 
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_performance_plan (performance_plan_id, performance_plan_desc, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(substr(concat(CAST(CAST((
          SELECT
              coalesce(max(performance_plan_id), CAST(0 as BIGNUMERIC))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_performance_plan
        ) + CAST(row_number() OVER (ORDER BY wrk.performance_plan_desc) as BIGNUMERIC) as INT64) as STRING) , repeat(' ', 10)), 1, 10) AS INT64) AS performance_plan_id,
        wrk.performance_plan_desc,
        wrk.source_system_code,
        wrk.dw_last_update_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_performance_plan_wrk AS wrk
      WHERE Upper(wrk.performance_plan_desc) NOT IN(
        SELECT
            Upper(performance_plan_desc)
          FROM
            {{ params.param_hr_core_dataset_name }}.ref_performance_plan
      )
  ;
/* Test Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Performance_Plan_Desc
        from {{ params.param_hr_core_dataset_name }}.ref_performance_plan 
        group by Performance_Plan_Desc
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.Ref_Performance_Plan');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
