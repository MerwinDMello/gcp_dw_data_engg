BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND);

  BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.recruitment_job_parameter_detail AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) FROM {{ params.param_hr_stage_dataset_name }}.rect_job_param_detail_wrk AS wrk WHERE wrk.recruitment_job_parameter_sid = tgt.recruitment_job_parameter_sid
   AND wrk.element_detail_entity_text = tgt.element_detail_entity_text
   AND wrk.element_detail_type_text = tgt.element_detail_type_text
   AND wrk.element_detail_seq = tgt.element_detail_seq
   AND (wrk.job_parameter_num <> tgt.job_parameter_num
   OR coalesce(CAST(wrk.element_detail_id AS STRING), 'X') <> coalesce(CAST(tgt.element_detail_id AS STRING), 'X')
   OR coalesce(trim(wrk.element_detail_value_text), 'XX') <> coalesce(trim(tgt.element_detail_value_text), 'XX'))
   AND (tgt.valid_to_date) = DATETIME('9999-12-31 23:59:59') ;

  INSERT INTO {{ params.param_hr_core_dataset_name }}.recruitment_job_parameter_detail (recruitment_job_parameter_sid, element_detail_entity_text, element_detail_type_text, element_detail_seq, valid_from_date, valid_to_date, job_parameter_num, element_detail_id, element_detail_value_text, source_system_code, dw_last_update_date_time)
  
  SELECT
        wrk.recruitment_job_parameter_sid,
        wrk.element_detail_entity_text,
        wrk.element_detail_type_text,
        wrk.element_detail_seq,
		current_dt,
        DATETIME('9999-12-31 23:59:59') AS valid_to_date,
        wrk.job_parameter_num,
        wrk.element_detail_id,
        wrk.element_detail_value_text,
        wrk.source_system_code,
        wrk.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.rect_job_param_detail_wrk AS wrk
      WHERE (wrk.recruitment_job_parameter_sid, wrk.element_detail_entity_text, wrk.element_detail_type_text, wrk.element_detail_seq, wrk.job_parameter_num, wrk.element_detail_id, COALESCE(wrk.element_detail_value_text,'')) NOT IN(
        SELECT AS STRUCT
            tgt.recruitment_job_parameter_sid,
            tgt.element_detail_entity_text,
            tgt.element_detail_type_text,
            tgt.element_detail_seq,
            tgt.job_parameter_num,
            tgt.element_detail_id,
            COALESCE(tgt.element_detail_value_text,'')
          FROM
            {{ params.param_hr_base_views_dataset_name }}.recruitment_job_parameter_detail AS tgt
          WHERE (tgt.valid_to_date) = DATETIME('9999-12-31 23:59:59') 
      );

      /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Recruitment_Job_Parameter_SID ,Element_Detail_Entity_Text ,Element_Detail_Type_Text ,Element_Detail_Seq ,Valid_From_Date 
        from {{ params.param_hr_core_dataset_name }}.recruitment_job_parameter_detail
        group by Recruitment_Job_Parameter_SID ,Element_Detail_Entity_Text ,Element_Detail_Type_Text ,Element_Detail_Seq ,Valid_From_Date 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.recruitment_job_parameter_detail');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;