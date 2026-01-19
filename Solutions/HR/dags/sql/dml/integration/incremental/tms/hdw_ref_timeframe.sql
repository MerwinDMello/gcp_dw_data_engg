BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_timeframe (timeframe_id, timeframe_desc, source_system_code, dw_last_update_date_time)
    SELECT
        row_number() OVER (ORDER BY stg.flight_risk_timeframe DESC) AS timeframe_id,
        stg.flight_risk_timeframe,
        'M' AS source_system_code,
        timestamp_trunc(current_datetime("US/Central"), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              coalesce(stg_0.flight_risk_timeframe,'') as flight_risk_timeframe
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee_info AS stg_0
              LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.ref_timeframe AS tgt 
              ON upper(coalesce(tgt.timeframe_desc, '')) = upper(coalesce(stg_0.flight_risk_timeframe, ''))
            WHERE tgt.timeframe_desc IS NULL
            GROUP BY 1
        ) AS stg
  ;
  
  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT timeframe_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_timeframe
      GROUP BY timeframe_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0  THEN
	  ROLLBACK TRANSACTION;
	  RAISE USING MESSAGE = CONCAT('Duplicates not allowed in table:{{ params.param_hr_core_dataset_name }}.ref_timeframe ');
	  ELSE 
	  COMMIT TRANSACTION;
  END IF;
END;