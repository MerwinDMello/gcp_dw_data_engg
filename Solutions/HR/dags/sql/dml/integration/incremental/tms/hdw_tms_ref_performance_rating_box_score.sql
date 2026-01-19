BEGIN
DECLARE DUP_COUNT INT64;


  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_performance_rating_box_score_wrk;

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_performance_rating_box_score_wrk (performance_rating_id, performance_potential_id, overall_performance_rating_desc, performance_potential_desc, box_score_num, box_score_desc, box_score_group_desc, source_system_code, dw_last_update_date_time)
    SELECT
        r.performance_rating_id AS performance_rating_id,
        r1.probability_potential_id AS performance_potential_id,
        hr.ovrall_calibrated_perf_rvw AS overall_performance_rating_desc,
        hr.tms_box_potential AS performance_potential_desc,
        safe_cast(hr.box_score as int64) AS box_score_num,
        hr.box_description AS box_score_desc,
        hr.box_bracket AS box_score_group_desc,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.map_tms_box AS hr
        INNER JOIN {{ params.param_hr_core_dataset_name }}.ref_performance_rating AS r ON hr.ovrall_calibrated_perf_rvw = r.performance_rating_desc
        INNER JOIN {{ params.param_hr_core_dataset_name }}.ref_probability_potential AS r1 ON hr.tms_box_potential = r1.probability_potential_desc
  ;

    SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT performance_rating_id, performance_potential_id
      FROM {{ params.param_hr_stage_dataset_name }}.ref_performance_rating_box_score_wrk
      GROUP BY performance_rating_id, performance_potential_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table:{{ params.param_hr_stage_dataset_name }}.ref_performance_rating_box_score_wrk ');
  ELSE
  COMMIT TRANSACTION;
  END IF;

 BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_performance_rating_box_score (performance_rating_id, performance_potential_id, overall_performance_rating_desc, performance_potential_desc, box_score_num, box_score_desc, box_score_group_desc, source_system_code, dw_last_update_date_time)
    SELECT
        w.performance_rating_id,
        w.performance_potential_id,
        w.overall_performance_rating_desc,
        w.performance_potential_desc,
        w.box_score_num,
        w.box_score_desc,
        w.box_score_group_desc,
        w.source_system_code,
        w.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_performance_rating_box_score_wrk AS w
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.ref_performance_rating_box_score AS c ON w.performance_rating_id = c.performance_rating_id
         AND w.performance_potential_id = c.performance_potential_id
      WHERE c.performance_rating_id IS NULL
       AND c.performance_potential_id IS NULL
  ;

      SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT performance_rating_id, performance_potential_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_performance_rating_box_score
      GROUP BY performance_rating_id, performance_potential_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.ref_performance_rating_box_score ');
  ELSE
  COMMIT TRANSACTION;
  END IF;
 
  END;
