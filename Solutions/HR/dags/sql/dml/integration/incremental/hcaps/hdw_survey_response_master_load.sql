BEGIN
  DECLARE DUP_COUNT INT64;
  declare  
    current_ts datetime;
  set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.survey_response_master_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.survey_response_master_wrk (eff_from_date, survey_question_sid, response_value_text, eff_to_date, response_label_text, source_system_code, dw_last_update_date_time)
    SELECT
        DATE '2000-01-01' AS eff_from_date,
        0 AS survey_question_sid,
        '0' AS response_value_text,
        DATE '9999-12-31' AS eff_to_date,
        'Response Label Unknown' AS response_label_text,
        'H' AS source_system_code,
        current_ts AS dw_last_update_date_time
  ;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.survey_response_master_wrk (eff_from_date, survey_question_sid, response_value_text, eff_to_date, response_label_text, source_system_code, dw_last_update_date_time)
    SELECT
        CASE
          WHEN (
            SELECT
                count(*)
              FROM
                {{ params.param_hr_base_views_dataset_name }}.survey_response_master
          ) = 0 THEN DATE '2000-01-01'
          ELSE DATE(current_ts)
        END AS eff_from_date,
        sq.survey_question_sid AS survey_question_sid,
        cast(hqm.response as string) AS response_value_text,
        DATE '9999-12-31' AS eff_to_date,
        hqm.response_label AS response_label_text,
        'H' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hdw_resp_lbl AS hqm
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON trim(hqm.question_id) = trim(sq.question_id)
        INNER JOIN {{ params.param_hr_core_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
      WHERE upper(rs.survey_group_text) = 'PATIENT_SATISFACTION'
       AND sq.eff_to_date = '9999-12-31'
       AND rs.eff_to_date = '9999-12-31'
  ;


  UPDATE {{ params.param_hr_core_dataset_name }}.survey_response_master AS tgt SET eff_to_date = DATE(DATE(current_ts) - INTERVAL 1 DAY), dw_last_update_date_time = wrk.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.survey_response_master_wrk AS wrk WHERE wrk.survey_question_sid = tgt.survey_question_sid
   AND trim(wrk.response_value_text) = trim(tgt.response_value_text)
   AND upper(trim(coalesce(wrk.response_label_text, ''))) <> upper(trim(coalesce(tgt.response_label_text, '')));

  BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.survey_response_master (survey_response_sid, eff_from_date, survey_question_sid, response_value_text, eff_to_date, response_label_text, source_system_code, dw_last_update_date_time)
    SELECT
        CASE
          WHEN survey_response_master_wrk.survey_question_sid = 0
           AND upper(survey_response_master_wrk.response_value_text) = '0' THEN CAST(0 as int64)
          ELSE (
            SELECT
                coalesce(max(survey_response_sid), CAST(0 as int64))
              FROM
                {{ params.param_hr_core_dataset_name }}.survey_response_master
          ) + CAST(row_number() OVER (ORDER BY survey_response_master_wrk.survey_question_sid, survey_response_master_wrk.response_value_text, survey_response_master_wrk.response_label_text) as int64)
        END AS survey_response_sid,
        survey_response_master_wrk.eff_from_date,
        survey_response_master_wrk.survey_question_sid,
        survey_response_master_wrk.response_value_text,
        survey_response_master_wrk.eff_to_date,
        survey_response_master_wrk.response_label_text,
        survey_response_master_wrk.source_system_code,
        survey_response_master_wrk.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.survey_response_master_wrk
      WHERE (survey_response_master_wrk.survey_question_sid, trim(survey_response_master_wrk.response_value_text), upper(trim(coalesce(survey_response_master_wrk.response_label_text, '')))) NOT IN(
        SELECT AS STRUCT
            survey_response_master.survey_question_sid,
            trim(survey_response_master.response_value_text),
            upper(trim(coalesce(survey_response_master.response_label_text, '')))
          FROM
            {{ params.param_hr_core_dataset_name }}.survey_response_master
      )
  ;
      SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            eff_from_date, survey_question_sid, response_value_text
        from {{ params.param_hr_core_dataset_name }}.survey_response_master
        group by eff_from_date, survey_question_sid, response_value_text
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat( 'Duplicates are not allowed in the table: survey_response_master' );
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;