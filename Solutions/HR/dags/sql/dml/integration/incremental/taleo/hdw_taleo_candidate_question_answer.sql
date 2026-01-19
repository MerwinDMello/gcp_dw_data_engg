BEGIN
	  DECLARE dup_count INT64;
	  DECLARE current_dt DATETIME;
	  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

	  BEGIN TRANSACTION;
  UPDATE {{ params.param_hr_core_dataset_name }}.candidate_question_answer AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND)FROM {{ params.param_hr_stage_dataset_name }}.candidate_question_answer_wrk AS stg WHERE tgt.question_answer_sid = stg.question_answer_sid
   AND (trim(CAST(coalesce(tgt.question_answer_num, -999) as STRING)) <> trim(CAST(coalesce(stg.question_answer_num, -999) as STRING))
   OR trim(CAST(coalesce(tgt.candidate_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.candidate_sid, -999) as STRING))
   OR trim(CAST(coalesce(tgt.creation_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.creation_date, DATE '1900-01-01') as STRING))
   OR trim(CAST(coalesce(tgt.question_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.question_sid, -999) as STRING))
   OR trim(CAST(coalesce(tgt.answer_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.answer_sid, -999) as STRING))
   OR upper(trim(coalesce(tgt.comment_text, 'XXX'))) <> upper(trim(coalesce(stg.comment_text, 'XXX')))
   OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(stg.source_system_code), 'X'))
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");

  INSERT INTO {{ params.param_hr_core_dataset_name }}.candidate_question_answer (question_answer_sid, question_answer_num, candidate_sid, valid_from_date, valid_to_date, creation_date, question_sid, answer_sid, comment_text, source_system_code, dw_last_update_date_time)
    SELECT
        stg.question_answer_sid,
        stg.question_answer_num,
        stg.candidate_sid,
        current_dt,
        DATETIME('9999-12-31 23:59:59') AS valid_to_date,
        stg.creation_date,
        stg.question_sid,
        stg.answer_sid,
        stg.comment_text,
        stg.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND)AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.candidate_question_answer_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question_answer AS tgt ON tgt.question_answer_sid = stg.question_answer_sid
         AND trim(CAST(coalesce(tgt.question_answer_num, -999) as STRING)) = trim(CAST(coalesce(stg.question_answer_num, -999) as STRING))
         AND trim(CAST(coalesce(tgt.candidate_sid, -999) as STRING)) = trim(CAST(coalesce(stg.candidate_sid, -999) as STRING))
         AND trim(CAST(coalesce(tgt.creation_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.creation_date, DATE '1900-01-01') as STRING))
         AND trim(CAST(coalesce(tgt.question_sid, -999) as STRING)) = trim(CAST(coalesce(stg.question_sid, -999) as STRING))
         AND trim(CAST(coalesce(tgt.answer_sid, -999) as STRING)) = trim(CAST(coalesce(stg.answer_sid, -999) as STRING))
         AND upper(trim(coalesce(tgt.comment_text, 'XXX'))) = upper(trim(coalesce(stg.comment_text, 'XXX')))
         AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
         AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
      WHERE tgt.question_answer_sid IS NULL
  ;

    /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Question_Answer_SID ,Valid_From_Date 
        from {{ params.param_hr_core_dataset_name }}.candidate_question_answer
        group by Question_Answer_SID ,Valid_From_Date 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.candidate_question_answer');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;