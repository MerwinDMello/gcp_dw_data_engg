BEGIN
  DECLARE DUP_COUNT INT64;
  declare  
    current_ts datetime;
  set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.survey_question_wrk;

  BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.survey_question_wrk (survey_sid, eff_from_date, survey_sub_category_text, question_id, question_type_code, question_short_name, question_desc, question_seq_num, top_box_num, top_box_high_num, measure_id_text, legacy_question_id, standard_flag, ignore_value, eff_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        coalesce(rs.survey_sid, 0) AS survey_sid,
        CASE
          WHEN (
            SELECT
                count(*)
              FROM
                {{ params.param_hr_base_views_dataset_name }}.survey_question
          ) = 0 THEN DATE '2000-01-01'
          ELSE DATE(current_ts)
        END AS eff_from_date,
        substr(sub_category, 1, 100) AS survey_sub_category_text,
        trim(hqm.question_id) AS question_id,
        'UNK' AS question_type_code,
        substr(qshort_txt, 1, 255) AS question_short_name,
        substr(qtxt, 1, 500) AS question_desc,
        coalesce(safe_cast(survey_ord as int64), 0) AS question_seq_num,
        tboxvalue AS top_box_num,
        tbox_high_val AS top_box_high_num,
        qlsx.measure_id_text,
        -- Commented the below line and added Coalesce
        -- ,QLSX.LEGACY_QUESTION_ID AS LEGACY_QUESTION_ID
        coalesce(qlsx.legacy_question_id, 0) AS legacy_question_id,
        hqm.std_flag AS standard_flag,
        CASE
           regexp_extract(trim(ignore_val), '([0-9]+)')
          WHEN '' THEN 0
          ELSE CAST(regexp_extract(trim(ignore_val), '([0-9]+)') as INT64)
        END AS ignore_value,
        /*IGNORE_VAL COL FROM SOURCE HAS SOME SPECIAL CHARACTERS(CARRIAGE RETURN), SO EXTRACTING ONLY NUMERIC PART TO AVOID BAD DATA ERROR*/
        DATE '9999-12-31' AS eff_to_date,
        'H' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_quest_mstr AS hqm
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_category_num = hqm.ptype
         AND rs.survey_category_code = hqm.qtype
         AND rs.survey_category_text = substr(hqm.category, 1, 50)
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.question_legacy_survey_xwalk AS qlsx ON hqm.question_id = qlsx.question_id
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
  ;
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            eff_from_date, survey_sid, question_id
        from {{ params.param_hr_stage_dataset_name }}.survey_question_wrk
        group by eff_from_date, survey_sid, question_id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat( 'Duplicates are not allowed in the table: survey_question_wrk' );
    ELSE
      COMMIT TRANSACTION;
    END IF;




 BEGIN TRANSACTION;
  UPDATE {{ params.param_hr_core_dataset_name }}.survey_question AS tgt SET survey_sub_category_text = wrk.survey_sub_category_text, question_desc = wrk.question_desc, question_type_code = wrk.question_type_code, question_short_name = wrk.question_short_name, question_seq_num = wrk.question_seq_num, top_box_num = wrk.top_box_num, top_box_high_num = wrk.top_box_high_num, measure_id_text = wrk.measure_id_text, legacy_question_id = wrk.legacy_question_id, standard_flag = wrk.standard_flag, ignore_value = wrk.ignore_value, dw_last_update_date_time = wrk.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.survey_question_wrk AS wrk WHERE wrk.survey_sid = tgt.survey_sid
   AND wrk.question_id = tgt.question_id
   AND (upper(trim(coalesce(wrk.survey_sub_category_text, ''))) <> upper(trim(coalesce(tgt.survey_sub_category_text, '')))
   OR upper(trim(coalesce(wrk.question_desc, ''))) <> upper(trim(coalesce(tgt.question_desc, '')))
   OR upper(trim(coalesce(wrk.question_type_code, ''))) <> upper(trim(coalesce(tgt.question_type_code, '')))
   OR upper(trim(coalesce(wrk.question_short_name, ''))) <> upper(trim(coalesce(tgt.question_short_name, '')))
   OR coalesce(wrk.question_seq_num, safe_cast('' as int64)) <> coalesce(tgt.question_seq_num, safe_cast('' as int64))
   OR coalesce(wrk.top_box_num, safe_cast('' as int64)) <> coalesce(tgt.top_box_num, safe_cast('' as int64))
   OR coalesce(wrk.top_box_high_num, safe_cast('' as int64)) <>coalesce(tgt.top_box_high_num, safe_cast('' as int64))
   OR upper(trim(coalesce(wrk.measure_id_text, ''))) <> upper(trim(coalesce(tgt.measure_id_text, '')))
   OR coalesce(wrk.legacy_question_id, safe_cast('' as int64)) <> coalesce(tgt.legacy_question_id, safe_cast('' as int64))
   OR upper(trim(coalesce(wrk.standard_flag, ''))) <> upper(trim(coalesce(tgt.standard_flag, '')))
   OR coalesce(wrk.ignore_value, safe_cast('' as int64)) <> coalesce(tgt.ignore_value, safe_cast('' as int64)))
   AND tgt.eff_to_date = '9999-12-31'
   AND upper(tgt.source_system_code) = 'H';

  INSERT INTO {{ params.param_hr_core_dataset_name }}.survey_question (survey_question_sid, survey_sid, eff_from_date, survey_sub_category_text, question_id, question_type_code, question_short_name, question_desc, question_seq_num, top_box_num, top_box_high_num, measure_id_text, legacy_question_id, standard_flag, ignore_value, eff_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              coalesce(max(survey_question_sid), CAST(0 as int64))
            FROM
              {{ params.param_hr_base_views_dataset_name }}.survey_question
        ) + CAST(row_number() OVER (ORDER BY wrk.question_id, wrk.survey_sub_category_text, wrk.question_desc) as int64) AS survey_question_sid,
        wrk.survey_sid,
        wrk.eff_from_date,
        wrk.survey_sub_category_text,
        wrk.question_id,
        wrk.question_type_code,
        wrk.question_short_name,
        wrk.question_desc,
        wrk.question_seq_num,
        wrk.top_box_num,
        wrk.top_box_high_num,
        wrk.measure_id_text,
        wrk.legacy_question_id,
        wrk.standard_flag,
        wrk.ignore_value,
        wrk.eff_to_date,
        wrk.source_system_code,
        wrk.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.survey_question_wrk AS wrk
      WHERE (wrk.survey_sid, wrk.question_id) NOT IN(
        SELECT AS STRUCT
            survey_question.survey_sid,
            survey_question.question_id
          FROM
            {{ params.param_hr_core_dataset_name }}.survey_question
          WHERE survey_question.eff_to_date = '9999-12-31'
           AND upper(survey_question.source_system_code) = 'H'
      )
  ;

      SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            eff_from_date, survey_sid, question_id
        from {{ params.param_hr_core_dataset_name }}.survey_question
        group by eff_from_date, survey_sid, question_id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat( 'Duplicates are not allowed in the table: survey_question' );
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;

