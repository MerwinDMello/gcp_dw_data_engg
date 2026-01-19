 BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt=current_datetime();

  BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.candidate_question AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = stg.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.candidate_question_wrk AS stg WHERE tgt.question_sid = stg.question_sid
   AND (trim(CAST(coalesce(tgt.question_num, -999) as STRING)) <> trim(CAST(coalesce(stg.question_num, -999) as STRING))
   OR trim(CAST(coalesce(tgt.creation_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.creation_date, DATE '1900-01-01') as STRING))
   OR upper(trim(coalesce(tgt.question_desc, 'XXX'))) <> upper(trim(coalesce(stg.question_desc, 'XXX')))
   OR upper(trim(coalesce(tgt.question_code, 'XXX'))) <> upper(trim(coalesce(stg.question_code, 'XXX')))
   OR trim(CAST(coalesce(tgt.last_modified_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.last_modified_date, DATE '1900-01-01') as STRING))
   OR trim(CAST(coalesce(tgt.requisition_num, -999) as STRING)) <> trim(CAST(coalesce(stg.requisition_num, -999) as STRING))
   OR trim(CAST(coalesce(tgt.question_type_num, -9) as STRING)) <> trim(CAST(coalesce(stg.question_type_num, -9) as STRING))
   OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(stg.source_system_code), 'X'))
   AND (tgt.valid_to_date) = DATETIME('9999-12-31 23:59:59') ;

  INSERT INTO {{ params.param_hr_core_dataset_name }}.candidate_question (question_sid, question_num, valid_from_date, valid_to_date, creation_date, question_desc, question_code, last_modified_date, requisition_num, question_type_num, source_system_code, dw_last_update_date_time)
    SELECT
        stg.question_sid,
        stg.question_num,
        current_dt,
        DATETIME('9999-12-31 23:59:59'),
        stg.creation_date,
        stg.question_desc,
        stg.question_code,
        stg.last_modified_date,
        stg.requisition_num,
        stg.question_type_num,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.candidate_question_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS tgt ON tgt.question_sid = stg.question_sid
         AND trim(CAST(coalesce(tgt.question_num, -999) as STRING)) = trim(CAST(coalesce(stg.question_num, -999) as STRING))
         AND trim(CAST(coalesce(tgt.creation_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.creation_date, DATE '1900-01-01') as STRING))
         AND upper(trim(coalesce(tgt.question_desc, 'XXX'))) = upper(trim(coalesce(stg.question_desc, 'XXX')))
         AND upper(trim(coalesce(tgt.question_code, 'XXX'))) = upper(trim(coalesce(stg.question_code, 'XXX')))
         AND trim(CAST(coalesce(tgt.last_modified_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.last_modified_date, DATE '1900-01-01') as STRING))
         AND trim(CAST(coalesce(tgt.requisition_num, -999) as STRING)) = trim(CAST(coalesce(stg.requisition_num, -999) as STRING))
         AND trim(CAST(coalesce(tgt.question_type_num, -9) as STRING)) = trim(CAST(coalesce(stg.question_type_num, -9) as STRING))
         AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
         AND (tgt.valid_to_date) = DATETIME('9999-12-31 23:59:59') 
      WHERE tgt.question_sid IS NULL
  ;



    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Question_SID ,Valid_From_Date
        from {{ params.param_hr_core_dataset_name }}.candidate_question
        group by Question_SID ,Valid_From_Date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table:{{ params.param_hr_core_dataset_name }}.candidate_question');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;