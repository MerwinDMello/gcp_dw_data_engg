

CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_questionanswer', '(QNS_Num)||\'-T\'', 'Candidate_Question_Answer');

/*  truncate work Table */

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_question_answer_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_question_answer_wrk (question_answer_sid, question_answer_num, candidate_sid, valid_from_date, creation_date, question_sid, answer_sid, comment_text, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(xwlk.sk  AS INT64)AS question_answer_sid,
        CAST(stg.qns_num AS INT64) AS question_answer_num,
        can.candidate_sid AS candidate_sid,
        stg.file_date AS valid_from_date,
        CASE
          WHEN trim(stg.creationdate) = '' THEN CAST(NULL as DATE)
          ELSE DATE(creationdate)
        END AS creation_date,
        canq.question_sid AS question_sid,
        cana.answer_sid AS answer_sid,
        stg.explanation AS comment_text,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_questionanswer AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(trim(stg.qns_num), '-T') = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(CASE
           trim(stg.candidate_number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.candidate_number) as INT64)
        END, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'T'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(CASE
           trim(stg.question_number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.question_number) as INT64)
        END, -9999) = coalesce(canq.question_num, -9999)
         AND upper(canq.source_system_code) = 'T'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(CASE
           trim(stg.answer_number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.answer_number) as INT64)
        END, -9999) = coalesce(cana.answer_num, -9999)
         AND upper(cana.source_system_code) = 'T'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

  ;


  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_question_answer_xwlk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_question_answer_xwlk (candidate, jobreqquestion, jobrequisition, jobreqquestionanswer, dw_last_update_date_time)
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A00', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionanswer)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE upper(ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_longtextvalue) <> ''
       OR ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionanswer <> 0
      GROUP BY 1, 2, 3, upper(concat('A00', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionanswer)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A01', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_1_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_1_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A01', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_1_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A02', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_2_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_2_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A02', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_2_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A03', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_3_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_3_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A03', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_3_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A04', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_4_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_4_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A04', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_4_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A05', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_5_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_5_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A05', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_5_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A06', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_6_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_6_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A06', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_6_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A07', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_7_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_7_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A07', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_7_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A08', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_8_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_8_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A08', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_8_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A09', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_9_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_9_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A09', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_9_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A10', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_10_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_10_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A10', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_10_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A11', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_11_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_11_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A11', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_11_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A12', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_12_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_12_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A12', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_12_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A13', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_13_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_13_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A13', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_13_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A14', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_14_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_14_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A14', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_14_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A15', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_15_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_15_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A15', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_15_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A16', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_16_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_16_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A16', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_16_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A17', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_17_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_17_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A17', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_17_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A18', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_18_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_18_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A18', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_18_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A19', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_19_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_19_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A19', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_19_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A20', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_20_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_20_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A20', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_20_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A21', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_21_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_21_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A21', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_21_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A22', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_22_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_22_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A22', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_22_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A23', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_23_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_23_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A23', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_23_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A24', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_24_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_24_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A24', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_24_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A25', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_25_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_25_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A25', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_25_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A26', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_26_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_26_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A26', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_26_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A27', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_27_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_27_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A27', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_27_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A28', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_28_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_28_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A28', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_28_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A29', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_29_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_29_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A29', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_29_)), 5
    UNION DISTINCT
    SELECT
        ats_cust_jobappquestionnaireresponse_stg.candidate,
        ats_cust_jobappquestionnaireresponse_stg.jobreqquestion,
        ats_cust_jobappquestionnaireresponse_stg.jobrequisition,
        max(concat('A30', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_30_)) AS jobreqquestionanswer,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg
      WHERE ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_30_ <> 0
      GROUP BY 1, 2, 3, upper(concat('A30', ats_cust_jobappquestionnaireresponse_stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_30_)), 5
  ;


CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'candidate_question_answer_xwlk', '((CANDIDATE))|| \'-\'||((JOBREQQUESTION))|| \'-\'||((JOBREQUISITION))|| \'-\'||(JOBREQQUESTIONANSWER)||\'-B\'', 'Candidate_Question_Answer');

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_question_answer_wrk (question_answer_sid, question_answer_num, candidate_sid, valid_from_date, creation_date, question_sid, answer_sid, comment_text, source_system_code, dw_last_update_date_time)
        SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionanswer
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A00', (stg.jobreqquestionansgroup_jobreqquestionanswer
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionanswer
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

      WHERE upper(stg.jobreqquestionansgroup_longtextvalue) <> ''
       OR stg.jobreqquestionansgroup_jobreqquestionanswer
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_1_ AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A01', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_1_), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_1_, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_1_ <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_2_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A02', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_2_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_2_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_2_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
   
     UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_3_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A03', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_3_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_3_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_3_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_4_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A04', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_4_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_4_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_4_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_5_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A05', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_5_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_5_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_5_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_6_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A06', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_6_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_6_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_6_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_7_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A07', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_7_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_7_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_7_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_8_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A08', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_8_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_8_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_8_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_9_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A09', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_9_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_9_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_9_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_10_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A10', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_10_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_10_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_10_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_11_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A11', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_11_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_11_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date) = DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_11_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_12_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A12', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_12_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_12_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_12_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_13_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A13', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_13_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_13_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_13_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_14_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A14', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_14_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_14_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_14_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_15_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A15', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_15_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_15_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_15_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_16_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A16', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_16_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_16_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_16_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_17_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A17', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_17_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_17_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_17_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_18_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A18', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_18_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_18_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_18_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_19_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A19', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_19_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_19_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_19_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_20_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A20', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_20_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_20_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_20_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_21_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A21', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_21_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_21_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_21_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_22_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A22', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_22_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_22_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_22_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_23_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A23', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_23_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_23_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_23_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_24_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A24', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_24_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_24_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_24_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_25_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A25', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_25_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_25_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_25_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_26_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A26', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_26_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_26_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_26_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_27_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A27', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_27_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_27_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_27_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_28_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A28', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_28_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_28_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_28_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_29_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A29', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_29_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_29_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_29_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION DISTINCT
    SELECT
        CAST(xwlk.sk AS INT64) AS question_answer_sid,
        stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_30_
 AS question_answer_num,
        can.candidate_sid,
        current_date() AS valid_from_date,
        DATE(NULL) AS creation_date,
        canq.question_sid,
        cana.answer_sid,
        stg.jobreqquestionansgroup_longtextvalue AS comment_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobappquestionnaireresponse_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.candidate), '-', (stg.jobreqquestion), '-', (stg.jobrequisition), '-A30', (stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_30_
), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION_ANSWER'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg.candidate, -9999) = coalesce(can.candidate_num, -9999)
         AND upper(can.source_system_code) = 'B'
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_question AS canq ON coalesce(stg.jobreqquestion, -9999) = coalesce(canq.question_num, -9999)
         AND coalesce(stg.jobrequisition, -9999) = coalesce(canq.requisition_num, -9999)
         AND upper(canq.source_system_code) = 'B'
         AND (canq.valid_to_date) = DATETIME("9999-12-31 23:59:59")

        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS cana ON coalesce(stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_30_
, -9999) = coalesce(cana.answer_num, -9999)
         AND cana.question_sid = canq.question_sid
         AND upper(cana.source_system_code) = 'B'
         AND (cana.valid_to_date)= DATETIME("9999-12-31 23:59:59")

      WHERE stg.jobreqquestionansgroup_jobreqquestionmultiselectanswer_jobreqquestionanswer_30_
 <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  ;

