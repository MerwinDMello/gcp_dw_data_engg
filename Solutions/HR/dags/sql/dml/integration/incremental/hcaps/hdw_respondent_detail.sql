BEGIN
/*  Load target Table with Sequence of Historical records */
declare  
    current_ts datetime;
set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

  INSERT INTO {{ params.param_hr_core_dataset_name }}.respondent_detail (respondent_id, survey_receive_date, respondent_type_code, survey_sid, source_system_code, dw_last_update_date_time)
    SELECT DISTINCT
        hpr.surv_id AS respondent_id,
        hpr.recdate AS survey_receive_date,
        'P' AS respondent_type_code,
        sq.survey_sid,
        'H' AS source_system_code,
        -- ,HPR.DW_Last_Update_Date_Time
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp AS hpr
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON sq.question_id = hpr.question_id
         AND upper(sq.source_system_code) = 'H'
        INNER JOIN /* Added New */
        {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
         AND rs.eff_to_date = sq.eff_to_date
      WHERE upper(rs.survey_group_text) = 'PATIENT_SATISFACTION'
       AND rs.eff_to_date = '9999-12-31'
       AND (hpr.surv_id, hpr.recdate, 'P', sq.survey_sid) NOT IN(
        SELECT AS STRUCT
            respondent_id,
            survey_receive_date,
            'P' as respondent_type_code,
            survey_sid
          FROM
            {{ params.param_hr_core_dataset_name }}.respondent_detail
      )
  ;
END ;
