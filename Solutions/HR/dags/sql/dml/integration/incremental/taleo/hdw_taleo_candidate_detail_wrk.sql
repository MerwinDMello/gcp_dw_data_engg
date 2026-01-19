/*  Generate the surrogate keys for Candidate */
-- CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','TALEO_CANDIDATEUDF','TRIM("Candidate_Number")', 'CANDIDATE_DETAIL');
call {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_candidateudf', 'trim(cast(Candidate_Number as string))', 'CANDIDATE');
--CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','TALEO_CANDIDATEUDF','TRIM(Candidate_Number)', 'candidate');

/*  Truncate Worktable Table     */

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_detail_wrk;

/*  Load Work Table with working Data */

INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_detail_wrk (file_date, candidate_sid, element_detail_entity_text, element_detail_type_text, element_detail_seq, valid_from_date, valid_to_date, element_detail_id, element_detail_value_text, source_system_code, dw_last_update_date_time)
SELECT
      a.file_date,
      cast(a.candidate_sid as int64),
      a.element_detail_entity_text,
      a.element_detail_type_text,
      a.element_detail_seq,
      a.valid_from_date,
      a.valid_to_date,
      a.element_detail_id,
      a.element_detail_value_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      (
SELECT
  stg.file_date,
  xwlk.sk AS candidate_sid,
  SUBSTR(MAX(TRIM(stg.udfdefinition_entity)), 1, 10) AS element_detail_entity_text,
  SUBSTR(MAX(TRIM(stg.udfdefinition_id)), 1, 250) AS element_detail_type_text,
  CASE
    WHEN (stg.sequence) IS NULL THEN 0
  ELSE
  CAST((stg.sequence) AS INT64)
END
  AS element_detail_seq,
  TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  (CASE
      WHEN (stg.udselement_number) IS NULL THEN NULL
    ELSE
    CAST((stg.udselement_number) AS NUMERIC)
  END
    ) AS element_detail_id,
  SUBSTR(MAX(TRIM(stg.value)), 1, 4000) AS element_detail_value_text,
  'T' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.taleo_candidateudf AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  (stg.candidate_number) = SAFE_CAST(xwlk.sk_source_txt AS int64)
  AND UPPER(xwlk.sk_type) = 'CANDIDATE'
GROUP BY
  1,
  2,
  UPPER(SUBSTR(TRIM(stg.udfdefinition_entity), 1, 10)),
  UPPER(SUBSTR(TRIM(stg.udfdefinition_id), 1, 250)),
  5,
  6,
  7,
  8,
  UPPER(SUBSTR(TRIM(stg.value), 1, 4000)),
  10,
  11
  UNION ALL
        SELECT
            -- ON (CAST( TRIM(STG."CANDIDATE_NUMBER")   AS VARCHAR(255)) = XWLK.SK_SOURCE_TXT    AND XWLK.SK_TYPE = 'CANDIDATE_DETAIL')
            -- -HDM2003
            current_date() AS file_date,
            c.candidate_sid AS candidate_sid,
            'CSUSER' AS element_detail_entity_text,
            dq.code AS element_detail_type_text,
            row_number() OVER (PARTITION BY c.candidate_sid, dq.code ORDER BY dan.creationdate) AS element_detail_seq,
            -- CAST(DA.SEQUENCE AS INTEGER)AS ELEMENT_DETAIL_SEQ,
            timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
                        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
            dan.possibleanswer_number AS element_detail_id,
            da.description AS element_detail_value_text,
            'T' AS source_system_code,
             DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
          FROM
            {{ params.param_hr_stage_dataset_name }}.diversityanswer_stg AS dan
            LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.diversityquestion_stg AS dq ON dq.diversityquestion_number = dan.question_number
            LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.diversitypossibleanswer_stg AS da ON da.diversitypossibleanswer_number = dan.possibleanswer_number
            LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.diversityquestiontype_stg AS qt ON dq.questiontype_number = qt.diversityquestiontype_number
            LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.diversitypossanstype_stg AS dpat ON dpat.diversitypossanstype_number = da.possibleanswertype_number
            INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS c ON dan.candidate_number = c.candidate_num
             AND (c.valid_to_date) = DATETIME("9999-12-31 23:59:59")
             AND upper(c.source_system_code) = 'T'
      ) AS a
    QUALIFY row_number() OVER (PARTITION BY a.candidate_sid, upper(a.element_detail_entity_text), upper(a.element_detail_type_text), a.element_detail_seq ORDER BY a.element_detail_id DESC) = 1
UNION ALL
    SELECT
        --  QUALIFY ROW_NUMBER() OVER(PARTITION BY C.CANDIDATE_SID,DQ.CODE, DA.SEQUENCE ORDER BY DAN.DW_LST_UPDT_DT DESC) = 1
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'FO_Job_Title' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(stg.hcafofamilyjob), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND trim(stg.hcafofamilyjob) <> ''
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq    
UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'FO_Country' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(stg.hcafofamilyrep), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND trim(stg.hcafofamilyrep) <> ''
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq      
UNION ALL
    SELECT
        -- HDM-1812
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        r'May we contact your current employer?' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(cast(stg1.permissiontocontact as string), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ats_edw_jobapplicationemploymenthistory_stg AS stg1 ON (stg.candidate) = (stg1.candidate)
      WHERE substr(cast(stg1.permissiontocontact as string), 1, 4000) IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, jobapplicationemploymenthistory) = element_detail_seq      
      UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'Gender' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(stg.eeocategories_gender_state), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND trim(stg.eeocategories_gender_state) <> ''
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
      UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'Ethnicity' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(stg.eeocategories_ethnicitycode), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND trim(stg.eeocategories_ethnicitycode) <> ''
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
      UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'Veteran Status' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(stg.hcapimilitary), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND trim(stg.hcapimilitary) <> ''
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
      UNION ALL
    SELECT
        -- -HDM-1812
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'WILLING_TO_RELOCATE' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(can.relocation_preference_code), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
      WHERE substr(trim(can.relocation_preference_code), 1, 4000) IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
      UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'Disability' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(stg.eeocategories_disabilityselfid_state), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND trim(stg.eeocategories_disabilityselfid_state) <> ''
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
      UNION ALL
    SELECT
        -- -HDM-1812
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'WILLING_TO_TRAVEL' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(can.travel_preference_code), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
      WHERE substr(trim(can.travel_preference_code), 1, 4000) IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
      UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'SALARY_DESIRED' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(cast(stg.jobapplicationpreferences_salaryexpectation as string), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND (stg.jobapplicationpreferences_salaryexpectation) IS NOT NULL 
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq

UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'DATE_AVAILABLE' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(CAST(stg.jobapplicationpreferences_availabilitydeclaration_specificdate AS STRING), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND (stg.jobapplicationpreferences_availabilitydeclaration_specificdate) IS NOT NULL 
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
UNION ALL
    SELECT
        -- -HDM-1812
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'currentDriverLicenseNumber' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(cp.driver_license_num), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_person AS cp ON can.candidate_sid = cp.candidate_sid
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
      WHERE substr(trim(cp.driver_license_num), 1, 4000) IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
      UNION ALL
    SELECT
        -- -HDM-1812
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'currentDriverLicenseState' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(cp.driver_license_state_code), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_person AS cp ON can.candidate_sid = cp.candidate_sid
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
      WHERE substr(trim(cp.driver_license_state_code), 1, 4000) IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
      UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'OVERTIME_WORK' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(stg.jobapplicationpreferences_preferences_workovertime_state), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND trim(stg.jobapplicationpreferences_preferences_workovertime_state) <> ''
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
      UNION ALL
    SELECT
        -- -HDM-1812
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'ADDRESS_STATE' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(a.state_code), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.junc_candidate_address AS ja ON ja.candidate_sid = can.candidate_sid
         AND (ja.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(ja.source_system_code) = 'B'
         AND upper(ja.addr_type_code) = 'CAN'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.address AS a ON a.addr_sid = ja.addr_sid
         AND upper(a.source_system_code) = 'B'
         AND upper(a.addr_type_code) = 'CAN'
      WHERE substr(trim(a.state_code), 1, 4000) IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
UNION ALL
    SELECT
        -- AND A.Valid_To_Date = '9999-12-31'
        -- -HDM-1812
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'UDF5' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(a.city_name), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.junc_candidate_address AS ja ON ja.candidate_sid = can.candidate_sid
         AND (ja.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(ja.source_system_code) = 'B'
         AND upper(ja.addr_type_code) = 'CAN'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.address AS a ON a.addr_sid = ja.addr_sid
         AND upper(a.source_system_code) = 'B'
         AND upper(a.addr_type_code) = 'CAN'
      WHERE substr(trim(a.city_name), 1, 4000) IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
UNION ALL
    SELECT
        -- AND A.Valid_To_Date = '9999-12-31'
        -- -HDM-1812
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'previouslyUsedName1' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(cp.maiden_name), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_person AS cp ON can.candidate_sid = cp.candidate_sid
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
      WHERE substr(trim(cp.maiden_name), 1, 4000) IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'Foreign_Official_Family' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(stg.hcafofamilyquestion), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND trim(stg.hcafofamilyquestion) <> ''
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'FO_Name' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(stg.hcafofamilyname), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND trim(stg.hcafofamilyname) <> ''
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        'Foreign_Official' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(stg.hcafoquestion), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND trim(stg.hcafoquestion) <> ''
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq
UNION ALL
    SELECT
        current_date() AS file_date,
        can.candidate_sid AS candidate_sid,
        'CSUSER' AS element_detail_entity_text,
        '3_4_ID' AS element_detail_type_text,
        1 AS element_detail_seq,
        timestamp_trunc(current_datetime('US/Central'), SECOND)  AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(NULL as NUMERIC) AS element_detail_id,
        substr(trim(stg.hcajobapplicationuid), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON (stg.candidate) = (can.candidate_num)
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
         AND trim(stg.hcajobapplicationuid) <> ''
      QUALIFY row_number() OVER (PARTITION BY stg.candidate ORDER BY stg.jobrequisition DESC, stg.jobapplication DESC) = element_detail_seq;  
	  
