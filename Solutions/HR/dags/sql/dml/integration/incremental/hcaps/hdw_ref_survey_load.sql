BEGIN
  DECLARE DUP_COUNT INT64;
declare  
    current_ts datetime;
set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

  BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_survey (survey_sid, eff_from_date, survey_category_num, survey_category_code, survey_category_text, eff_to_date, survey_group_text, source_system_code, dw_last_update_date_time)
    SELECT
        x.*
      FROM
        (
          SELECT
              0 AS survey_sid,
              DATE(current_ts) AS eff_from_date,
              0 AS survey_category_num,
              'UNK' AS survey_category_code,
              'UNKNOWN' AS survey_category_text,
              DATE '9999-12-31' AS eff_to_date,
              'PATIENT_SATISFACTION' AS survey_group_text,
              'H' AS source_system_code,
              current_ts AS dw_last_update_date_time
        ) AS x
      WHERE (trim(CAST(x.survey_category_num as STRING)), upper(trim(x.survey_category_code)), upper(trim(x.survey_category_text))) NOT IN(
        SELECT AS STRUCT
            trim(CAST(ref_survey.survey_category_num as STRING)) AS survey_category_num,
            upper(trim(ref_survey.survey_category_code)) AS survey_category_code,
            upper(trim(ref_survey.survey_category_text)) AS survey_category_text
          FROM
            {{ params.param_hr_core_dataset_name }}.ref_survey
          WHERE ref_survey.eff_to_date = DATE '9999-12-31'
      )
  ;
      SET DUP_COUNT = ( 
        select count(*)
        from (
        select
           survey_sid,  eff_from_date 
        from {{ params.param_hr_core_dataset_name }}.ref_survey
        group by survey_sid,  eff_from_date 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat( 'Duplicates are not allowed in the table: ref_survey' );
    ELSE
      COMMIT TRANSACTION;
    END IF;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_survey_wrk;


  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_survey_wrk (eff_from_date, survey_category_num, survey_category_code, survey_category_text, eff_to_date, survey_group_text, source_system_code, dw_last_update_date_time)
    SELECT
        CASE
          WHEN (
            SELECT
                count(*)
              FROM
                {{ params.param_hr_core_dataset_name }}.ref_survey
          ) = 0 THEN DATE '2000-01-01'
          ELSE DATE(current_ts)
        END AS eff_from_date,
        a.ptype AS survey_category_num,
        a.qtype AS survey_category_code,
        a.category AS survey_category_text,
        DATE '9999-12-31' AS eff_to_date,
        'Patient_Satisfaction' AS survey_group_text,
        'H' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              hca_quest_mstr.ptype,
              hca_quest_mstr.qtype,
              hca_quest_mstr.category
            FROM
              {{ params.param_hr_stage_dataset_name }}.hca_quest_mstr
        ) AS a
  ;

 BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_survey (survey_sid, eff_from_date, survey_category_num, survey_category_code, survey_category_text, eff_to_date, survey_group_text, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              coalesce(max(survey_sid), CAST(0 as int64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_survey
        ) + CAST(row_number() OVER (ORDER BY wrk.survey_category_num, wrk.survey_category_code, wrk.survey_category_text) as int64) AS survey_sid,
        wrk.eff_from_date,
        wrk.survey_category_num,
        wrk.survey_category_code,
        wrk.survey_category_text,
        wrk.eff_to_date,
        wrk.survey_group_text,
        wrk.source_system_code,
        wrk.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_survey_wrk AS wrk
      WHERE (wrk.survey_category_num, upper(trim(wrk.survey_category_code)), upper(trim(wrk.survey_category_text))) NOT IN(
        SELECT AS STRUCT
            ref_survey.survey_category_num AS survey_category_num,
            upper(trim(ref_survey.survey_category_code)) AS survey_category_code,
            upper(trim(ref_survey.survey_category_text)) AS survey_category_text
          FROM
            {{ params.param_hr_core_dataset_name }}.ref_survey
          WHERE ref_survey.eff_to_date = '9999-12-31'
      )
  ;
        SET DUP_COUNT = ( 
        select count(*)
        from (
        select
           survey_sid,  eff_from_date 
        from {{ params.param_hr_core_dataset_name }}.ref_survey
        group by survey_sid,  eff_from_date 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat( 'Duplicates are not allowed in the table: ref_survey' );
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
