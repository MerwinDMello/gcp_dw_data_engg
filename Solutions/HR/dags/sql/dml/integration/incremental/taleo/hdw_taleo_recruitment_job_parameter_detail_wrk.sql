/*  Generate the surrogate keys for Candidate */

CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_req_offerparameterudf', 'CAST(ReqOfferParameter_Number AS STRING)', 'RECRUITMENT_JOB_PARAMETER_DETAIL');

/*  Truncate Worktable Table     */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.rect_job_param_detail_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.rect_job_param_detail_wrk (file_date, recruitment_job_parameter_sid, element_detail_entity_text, element_detail_type_text, element_detail_seq, job_parameter_num, element_detail_id, element_detail_value_text, source_system_code, dw_last_update_date_time)
    SELECT
        stg.file_date,
        CAST(xwlk.sk AS INT64) AS recruitment_job_parameter_sid,
        stg.udfdefinition_entity AS element_detail_entity_text,
        stg.udfdefinition_id AS element_detail_type_text,
        stg.sequence AS element_detail_seq,
        stg.reqofferparameter_number AS job_parameter_num,
        stg.udselement_number AS element_detail_id,
        stg.value AS element_detail_value_text,
        'T' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), second) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_req_offerparameterudf AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON CAST(stg.reqofferparameter_number AS STRING) = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'RECRUITMENT_JOB_PARAMETER_DETAIL'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
  ;