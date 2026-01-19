BEGIN
  DECLARE DUP_COUNT INT64;

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.survey_question_wrk;
  
  BEGIN TRANSACTION;  
  
/* Load Work Table with working Data */


  INSERT INTO {{ params.param_hr_core_dataset_name }}.survey_question (survey_question_sid, eff_from_date, survey_sid, question_id, question_type_code, question_desc, question_seq_num, eff_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(row_number() OVER (ORDER BY survey_cycle_uuid) as INT64 ) + (
          SELECT
              coalesce(max(survey_question_sid), CAST(0 as INT64 ))
            FROM
              {{ params.param_hr_base_views_dataset_name }}.survey_question
        ) AS survey_question_sid,
        CURRENT_DATE('US/Central') AS eff_from_date,
        coalesce(rs.survey_sid, 0) AS survey_sid,
        question_label AS question_id,
        rqt.question_type_code AS question_type_code,
        question_text AS question_desc,
        question_order AS question_seq_num,
        DATE ("9999-12-31") AS eff_to_date,
        'G' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.glint_question AS gq
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_category_code = gq.survey_cycle_uuid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_question_type AS rqt ON gq.question_type = rqt.question_type_desc
        LEFT JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question sq
        ON coalesce(rs.survey_sid, 0) = coalesce(sq.survey_sid, 0)
        AND sq.question_id = question_label
        AND sq.eff_to_date = DATE ("9999-12-31")
        AND Trim(sq.source_system_code) = 'G'
        WHERE sq.question_id IS NULL
        ;

/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Survey_Question_SID,Eff_From_Date 
        from {{ params.param_hr_core_dataset_name }}.survey_question
        group by Survey_Question_SID,Eff_From_Date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = 'Duplicates not allowed in table:{{ params.param_hr_core_dataset_name }}.survey_question';
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;