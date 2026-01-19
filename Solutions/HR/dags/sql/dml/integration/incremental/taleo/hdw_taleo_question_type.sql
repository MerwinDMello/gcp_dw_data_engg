  BEGIN
  DECLARE dup_count INT64;

  BEGIN TRANSACTION;
  /*  Load Ref table  */
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_candidate_question_type AS tgt USING (
    SELECT
        qt_num AS question_type_num,
        description AS question_type_desc,
        'T' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), second) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_questiontype
      GROUP BY 1, 2, 3, 4
  ) AS src
  ON tgt.question_type_num = cast(src.question_type_num as INT64)
     WHEN MATCHED THEN UPDATE SET question_type_desc = src.question_type_desc, source_system_code = src.source_system_code, dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (question_type_num, question_type_desc, source_system_code, dw_last_update_date_time) VALUES (cast(src.question_type_num as INT64), src.question_type_desc, src.source_system_code, src.dw_last_update_date_time)
  ;


    /* Test Unique Index constraint set in Terdata */
    SET dup_count = ( 
        select count(*)
        from (
        select
            question_type_num
        from {{ params.param_hr_core_dataset_name }}.ref_candidate_question_type
        group by question_type_num
        having count(*) > 1
        )
    );
    IF dup_count <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_candidate_question_type');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;