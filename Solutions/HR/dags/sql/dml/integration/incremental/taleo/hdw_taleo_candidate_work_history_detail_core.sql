BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

  BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.candidate_work_history_detail AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) FROM {{ params.param_hr_stage_dataset_name }}.candidate_work_history_detail_wrk AS stg WHERE tgt.candidate_work_history_sid = stg.candidate_work_history_sid
   AND tgt.element_detail_entity_text = stg.element_detail_entity_text
   AND tgt.element_detail_type_text = stg.element_detail_type_text
   AND tgt.element_detail_seq_num = stg.element_detail_seq_num
   AND (trim(CAST(coalesce(tgt.element_detail_id, -999) as STRING)) <> trim(CAST(coalesce(stg.element_detail_id, -999) as STRING))
   OR upper(trim(coalesce(tgt.element_detail_value_text, 'A'))) <> upper(trim(coalesce(stg.element_detail_value_text, 'A'))))
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");

  INSERT INTO {{ params.param_hr_core_dataset_name }}.candidate_work_history_detail (candidate_work_history_sid, element_detail_entity_text, element_detail_type_text, element_detail_seq_num, valid_from_date, valid_to_date, element_detail_id, element_detail_value_text, source_system_code, dw_last_update_date_time)
    SELECT
        stg.candidate_work_history_sid,
        stg.element_detail_entity_text,
        stg.element_detail_type_text,
        stg.element_detail_seq_num,
        current_dt as valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.element_detail_id,
        stg.element_detail_value_text,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.candidate_work_history_detail_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history_detail AS tgt ON tgt.candidate_work_history_sid = stg.candidate_work_history_sid
         AND tgt.element_detail_entity_text = stg.element_detail_entity_text
         AND tgt.element_detail_type_text = stg.element_detail_type_text
         AND tgt.element_detail_seq_num = stg.element_detail_seq_num
         AND trim(CAST(coalesce(tgt.element_detail_id, -999) as STRING)) = trim(CAST(coalesce(stg.element_detail_id, -999) as STRING))
         AND upper(trim(coalesce(tgt.element_detail_value_text, 'A'))) = upper(trim(coalesce(stg.element_detail_value_text, 'A')))
         AND tgt.valid_to_date =  DATETIME("9999-12-31 23:59:59")
      WHERE tgt.candidate_work_history_sid IS NULL
  ;

    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Candidate_Work_History_SID ,Element_Detail_Entity_Text ,Element_Detail_Type_Text ,Element_Detail_Seq_Num ,Valid_From_Date
        from {{ params.param_hr_core_dataset_name }}.candidate_work_history_detail
        group by Candidate_Work_History_SID ,Element_Detail_Entity_Text ,Element_Detail_Type_Text ,Element_Detail_Seq_Num ,Valid_From_Date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.candidate_work_history_detail');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
