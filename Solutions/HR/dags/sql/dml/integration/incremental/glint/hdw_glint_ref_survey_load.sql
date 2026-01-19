  BEGIN
  DECLARE DUP_COUNT INT64;
  
/*  Truncate Worktable Table */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_glint_survey_wrk;


/* Load Work Table with working Data */
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_glint_survey_wrk (eff_from_date, survey_category_num, survey_category_code, survey_category_text, eff_to_date, survey_group_text, survey_date, survey_start_date, survey_end_date, source_system_code, dw_last_update_date_time)
    SELECT
        CURRENT_DATE('US/Central') AS eff_from_date,
        0 AS survey_category_num,
        a.survey_cycle_uuid AS survey_category_code,
        a.survey_cycle_title AS survey_category_text,
        DATE ("9999-12-31") AS eff_to_date,
        'Employee_Engagement' AS survey_group_text,
        parse_date(' %B %d, %Y', concat(split(a.survey_cycle_title, ' ')[SAFE_ORDINAL(1)], ' ', '01, ', split(a.survey_cycle_title, ' ')[SAFE_ORDINAL(2)])),
        a.survey_start_date,
        a.survey_end_date,
        'G' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              glint_response.survey_cycle_uuid,
              glint_response.survey_cycle_title,
              parse_date(' %B %d, %Y', concat(split(glint_response.survey_cycle_title, ' ')[SAFE_ORDINAL(1)], ' ', '01, ', split(glint_response.survey_cycle_title, ' ')[SAFE_ORDINAL(2)])) AS survey_date,
              min(date(glint_response.survey_creation_date)) AS survey_start_date,
              max(date(glint_response.survey_completion_date)) AS survey_end_date
            FROM
              {{ params.param_hr_stage_dataset_name }}.glint_response
            GROUP BY 1, 2, 3
        ) AS a
  ;


  BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_survey (survey_sid, eff_from_date, survey_category_num, survey_category_code, survey_category_text, eff_to_date, survey_group_text, survey_date, survey_start_date, survey_end_date, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              coalesce(max(survey_sid), CAST(0 as INT64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_survey
        ) + CAST(row_number() OVER (ORDER BY wrk.survey_category_code) as INT64) AS survey_sid,
        wrk.eff_from_date,
        wrk.survey_category_num,
        wrk.survey_category_code,
        wrk.survey_category_text,
        wrk.eff_to_date,
        wrk.survey_group_text,
        wrk.survey_date,
        wrk.survey_start_date,
        wrk.survey_end_date,
        wrk.source_system_code,
        wrk.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_glint_survey_wrk AS wrk
      WHERE upper(trim(wrk.survey_category_code)) NOT IN(
        SELECT
            upper(trim(survey_category_code))
          FROM
            {{ params.param_hr_core_dataset_name }}.ref_survey
          WHERE ref_survey.eff_to_date = DATE ("9999-12-31")
      )
  ;


    /* Test Unique Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            survey_sid ,eff_from_date
        from {{ params.param_hr_core_dataset_name }}.ref_survey
        group by survey_sid ,eff_from_date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_survey');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;