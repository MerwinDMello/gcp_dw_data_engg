BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;

  INSERT INTO {{ params.param_hr_core_dataset_name }}.survey_response (survey_question_sid, respondent_id, survey_receive_date, survey_mode_code, survey_response_sid, response_value_text, response_comment_text, final_record_ind, source_system_code, dw_last_update_date_time)
    SELECT
        sq.survey_question_sid,
        rd.respondent_id,
        rd.survey_receive_date,
        '0',  
        0,
        cast(gr.response as string),
        gr.comments,
        'Y' AS final_record_ind,
        'G' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.glint_response AS gr
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON gr.survey_cycle_uuid = rs.survey_category_code
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON sq.survey_sid = rs.survey_sid
         AND sq.question_id = gr.question
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.respondent_detail AS rd ON rs.survey_category_code = gr.survey_cycle_uuid
         AND rs.survey_sid = rd.survey_sid
         AND rd.employee_num = gr.employee_num
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_response AS sr ON sr.survey_question_sid = sq.survey_question_sid
         AND sr.respondent_id = rd.respondent_id
         AND sr.survey_receive_date = rd.survey_receive_date
      WHERE sr.survey_question_sid IS NULL
       AND sr.respondent_id IS NULL
       AND sr.survey_receive_date IS NULL
       QUALIFY ROW_NUMBER() OVER (PARTITION BY sq.survey_question_sid, rd.respondent_id, rd.survey_receive_date ORDER BY gr.response) = 1;

/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Survey_Question_SID ,Respondent_Id ,Survey_Receive_Date ,Survey_Mode_Code ,Survey_Response_SID 
        from {{ params.param_hr_core_dataset_name }}.survey_response
        group by Survey_Question_SID ,Respondent_Id ,Survey_Receive_Date ,Survey_Mode_Code ,Survey_Response_SID 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = 'Duplicates not allowed in table:{{ params.param_hr_core_dataset_name }}.survey_response';
    ELSE
      COMMIT TRANSACTION;
    END IF;END;