BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt=DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

  BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.candidate_answer AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = stg.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.candidate_answer_wrk AS stg WHERE tgt.answer_sid = stg.answer_sid
   AND (trim(CAST(coalesce(tgt.answer_num, -999) as STRING)) <> trim(CAST(coalesce(stg.answer_num, -999) as STRING))
   OR upper(trim(coalesce(tgt.answer_desc, 'XXX'))) <> upper(trim(coalesce(stg.answer_desc, 'XXX')))
   OR trim(CAST(coalesce(tgt.question_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.question_sid, -999) as STRING))
   OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(stg.source_system_code), 'X'))
   AND (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59");

  INSERT INTO {{ params.param_hr_core_dataset_name }}.candidate_answer (answer_sid, answer_num, valid_from_date, valid_to_date, answer_desc, question_sid, source_system_code, dw_last_update_date_time)
    SELECT
        stg.answer_sid,
        stg.answer_num,
        current_dt,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.answer_desc,
        stg.question_sid,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.candidate_answer_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS tgt ON tgt.answer_sid = stg.answer_sid
         AND trim(CAST(coalesce(tgt.answer_num, -999) as STRING)) = trim(CAST(coalesce(stg.answer_num, -999) as STRING))
         AND upper(trim(coalesce(tgt.answer_desc, 'XXX'))) = upper(trim(coalesce(stg.answer_desc, 'XXX')))
         AND trim(CAST(coalesce(tgt.question_sid, -999) as STRING)) = trim(CAST(coalesce(stg.question_sid, -999) as STRING))
         AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
         AND (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59")
      WHERE tgt.answer_sid IS NULL
  ;

    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Answer_SID ,Valid_From_Date
        from {{ params.param_hr_core_dataset_name }}.candidate_answer
        group by Answer_SID ,Valid_From_Date 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.candidate_answer');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;