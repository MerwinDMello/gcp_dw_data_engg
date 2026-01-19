CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_possibleanswer', 'TRIM(Ans_Num)||\'-T\'', 'CANDIDATE_ANSWER');

  
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_answer_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_answer_wrk (answer_sid, answer_num, valid_from_date, valid_to_date, answer_desc, question_sid, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(xwlk.sk AS INT64) AS answer_sid,
        CAST(stg.ans_num AS INT64) AS answer_num,
        DATETIME(stg.file_date) AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.description AS answer_desc,
        cq.question_sid,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_possibleanswer AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(trim(stg.ans_num), '-T') = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'CANDIDATE_ANSWER'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS cq ON CAST(stg.question_number AS INT64) = cq.question_num
        AND (cq.valid_to_date) =DATETIME("9999-12-31 23:59:59")
        AND upper(cq.source_system_code) = 'T'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
  ;
CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'ats_cust_jobreqquestionanswer_stg', '((JOBREQUISITION))|| \'-\' ||((JOBREQQUESTION ))|| \'-\' ||((JOBREQQUESTIONANSWER))||\'-B\'', 'CANDIDATE_ANSWER');

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_answer_wrk (answer_sid, answer_num, valid_from_date, valid_to_date, answer_desc, question_sid, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(xwlk.sk AS INT64) AS answer_sid,
        stg.jobreqquestionanswer AS answer_num,
        CURRENT_DATETIME('US/Central') AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        trim(stg.questionoption) AS answer_desc,
        cq.question_sid,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobreqquestionanswer_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.jobrequisition), '-', (stg.jobreqquestion), '-', (stg.jobreqquestionanswer), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_ANSWER'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS cq ON stg.jobreqquestion = cq.question_num
         AND stg.jobrequisition = cq.requisition_num
        AND (cq.valid_to_date) =DATETIME("9999-12-31 23:59:59")
        AND upper(cq.source_system_code) = 'B'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
  ;
  