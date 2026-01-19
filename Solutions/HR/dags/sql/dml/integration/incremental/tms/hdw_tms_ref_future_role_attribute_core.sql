BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;
  DELETE FROM {{ params.param_hr_stage_dataset_name }}.ref_future_role_attribute_wrk WHERE ref_future_role_attribute_wrk.future_role_attribute_desc IN(
    '--------', ''
  );

  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_future_role_attribute (future_role_attribute_id, future_role_attribute_desc, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(substr(concat(CAST(CAST((
          SELECT
              coalesce(max(future_role_attribute_id), CAST(0 as BIGNUMERIC))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_future_role_attribute
        ) + CAST(row_number() OVER (ORDER BY wrk.future_role_attribute_desc) as BIGNUMERIC) as INT64) as STRING), repeat(' ', 10)), 1, 10) AS INT64) AS future_role_attribute_id,
        wrk.future_role_attribute_desc,
        wrk.source_system_code,
        wrk.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_future_role_attribute_wrk AS wrk
      WHERE wrk.future_role_attribute_desc NOT IN(
        SELECT
            future_role_attribute_desc
          FROM
            {{ params.param_hr_core_dataset_name }}.ref_future_role_attribute AS ref_future_role_attribute_0
      )
;

  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT future_role_attribute_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_future_role_attribute
      GROUP BY future_role_attribute_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.ref_future_role_attribute ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;
END;