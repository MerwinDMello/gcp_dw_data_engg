CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_question', 'TRIM(Question_Num)||\'-T\'', 'CANDIDATE_QUESTION');

/*  truncate work Table */

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_question_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_question_wrk (question_sid, question_num, valid_from_date, valid_to_date, creation_date, question_desc, question_code, last_modified_date, requisition_num, question_type_num, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(xwlk.sk AS INT64) AS question_sid,
        CAST(stg.question_num AS INT64) AS question_num,
        stg.file_date AS valid_from_date,
        DATETIME('9999-12-31 23:59:59') AS valid_to_date,
        CASE
          WHEN trim(stg.creationdate) = '' THEN CAST(NULL as DATE)
          ELSE parse_date('%Y-%m-%d', substr(trim(stg.creationdate), 1, 10))
        END AS creation_date,
        stg.description AS question_desc,
        stg.question_name AS question_code,
        CASE
          WHEN trim(stg.lastmodifieddate) = '' THEN CAST(NULL as DATE)
          ELSE parse_date('%Y-%m-%d', substr(trim(stg.lastmodifieddate), 1, 10))
        END AS last_modified_date,
        NULL AS requisition_num,
        CAST(stg.typeofquestion_number AS INT64) AS question_type_num,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND)AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_question AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(trim(stg.question_num), '-T') = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
  ;

CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'ats_cust_jobreqquestion_stg', '((JOBREQQUESTION))|| \'-\'||((JOBREQUISITION))||\'-B\'', 'CANDIDATE_QUESTION');

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_question_wrk (question_sid, question_num, valid_from_date, valid_to_date, creation_date, question_desc, question_code, last_modified_date, requisition_num, question_type_num, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(xwlk.sk AS INT64) AS question_sid,
        cast(stg.jobreqquestion as int64) AS question_num,
        current_date() AS valid_from_date,
        DATETIME('9999-12-31 23:59:59')  AS valid_to_date,
        DATE(NULL) AS creation_date,
        LEFT(REGEXP_REPLACE(TRIM(STG.question_questiontext), r'([^\p{ASCII}]+)', ''),250) AS question_desc,
        CAST(stg.catalogquestion AS STRING) AS question_code,
        DATE(NULL) AS last_modified_date,
        cast(stg.jobrequisition as int64) AS requisition_num,
        1 AS question_type_num,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND)AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobreqquestion_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat((stg.jobreqquestion), '-', (stg.jobrequisition), '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_QUESTION'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
  ;