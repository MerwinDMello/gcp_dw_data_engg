BEGIN
DECLARE
  DUP_COUNT INT64;
  DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
BEGIN TRANSACTION;


/*   Detactive the Records  */

  UPDATE {{ params.param_hr_core_dataset_name }}.ref_phone_mode_adjustment AS empl SET eff_to_date = DATE(stg.eff_from_date - INTERVAL 1 DAY), dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.ref_phone_mode_adjustment AS stg WHERE empl.measure_id_text = stg.measure_id_text
   AND (coalesce(empl.mode_adjustment_amt, NUMERIC '0.000') <> coalesce(stg.mode_adjustment_amt, NUMERIC '0.000')
   OR coalesce(empl.bottom_mode_adjustment_amt, NUMERIC '0.000') <> coalesce(stg.bottom_mode_adjustment_amt, NUMERIC '0.000'))
   AND empl.eff_to_date = DATE '9999-12-31';


/**   Load core table   */

  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_phone_mode_adjustment (measure_id_text, eff_from_date, mode_adjustment_amt, bottom_mode_adjustment_amt, eff_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        stg.measure_id_text,
        stg.eff_from_date,
        stg.mode_adjustment_amt,
        stg.bottom_mode_adjustment_amt,
        stg.eff_to_date,
        stg.source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_phone_mode_adjustment AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.ref_phone_mode_adjustment AS tgt ON tgt.measure_id_text = stg.measure_id_text
         AND coalesce(tgt.mode_adjustment_amt, NUMERIC '0.000') = coalesce(stg.mode_adjustment_amt, NUMERIC '0.000')
         AND coalesce(tgt.bottom_mode_adjustment_amt, NUMERIC '0.000') = coalesce(stg.bottom_mode_adjustment_amt, NUMERIC '0.000')
         AND tgt.eff_to_date = DATE '9999-12-31'
      WHERE tgt.measure_id_text IS NULL
  ;
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       Measure_Id_Text , Eff_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_phone_mode_adjustment
    GROUP BY
       Measure_Id_Text , Eff_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: .edwhr_copy.ref_phone_mode_adjustment');
  ELSE
COMMIT TRANSACTION;

END IF;
END;