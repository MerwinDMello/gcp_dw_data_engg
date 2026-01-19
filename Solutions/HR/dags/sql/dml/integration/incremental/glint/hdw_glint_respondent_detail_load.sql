BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;

  INSERT INTO {{ params.param_hr_core_dataset_name }}.respondent_detail (respondent_id, survey_receive_date, respondent_type_code, survey_sid, employee_num, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(row_number() OVER (ORDER BY a.employee_num, a.survey_sid) as NUMERIC) + (
          SELECT
              coalesce(max(respondent_id), CAST(0 as NUMERIC))
            FROM
              {{ params.param_hr_base_views_dataset_name }}.respondent_detail
        ) AS respondent_id,
        a.survey_receive_date,
        'E' AS respondent_type_code,
        a.survey_sid,
        a.employee_num,
        'G' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              CAST(substr(survey_creation_date, 1, 10) as DATE) AS survey_receive_date,
              rs.survey_sid,
              gr.employee_num
            FROM
              {{ params.param_hr_stage_dataset_name }}.glint_response AS gr
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON gr.survey_cycle_uuid = rs.survey_category_code
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.respondent_detail AS rd ON rd.employee_num = gr.employee_num
               AND rs.survey_sid = rd.survey_sid
            WHERE rd.employee_num IS NULL
             AND rd.survey_sid IS NULL
             AND (upper(gr.employee_num) LIKE 'UK%'
             OR upper(gr.employee_num) LIKE 'HEA%'
             OR substr(gr.employee_num, 1, 1) IN(
              '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'
            ))
            GROUP BY 1, 2, 3
        ) AS a;

/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Respondent_Id ,Survey_Receive_Date ,Respondent_Type_Code ,Survey_SID
        from {{ params.param_hr_core_dataset_name }}.respondent_detail
        group by Respondent_Id ,Survey_Receive_Date ,Respondent_Type_Code ,Survey_SID
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = 'Duplicates not allowed in table:{{ params.param_hr_core_dataset_name }}.respondent_detail';
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;