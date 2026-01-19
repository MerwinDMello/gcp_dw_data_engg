
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_work_history_detail_wrk;

/*  LOAD WORK TABLE WITH WORKING DATA */
BEGIN

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_work_history_detail_wrk (file_date, candidate_work_history_sid, element_detail_entity_text, element_detail_type_text, element_detail_seq_num, valid_from_date, valid_to_date, element_detail_id, element_detail_value_text, source_system_code, dw_last_update_date_time)
    SELECT DISTINCT
        -- needs uppercase
        stg.file_date,
        cwh.candidate_work_history_sid AS candidate_work_history_sid,
        stg.udfdefinition_entity AS element_detail_entity_text,
        stg.udfdefinition_id AS element_detail_type_text,
        CAST(stg.sequence as INT64) AS element_detail_seq_num,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(stg.udselement_number as INT64) AS element_detail_id,
        stg.value AS element_detail_value_text,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_experienceudf AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS cwh ON stg.experience_number = cwh.candidate_work_history_num
         AND DATE(cwh.valid_to_date) = '9999-12-31'
         AND upper(cwh.source_system_code) = 'T'
  ;


  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_work_history_detail_wrk (file_date, candidate_work_history_sid, element_detail_entity_text, element_detail_type_text, element_detail_seq_num, valid_from_date, valid_to_date, element_detail_id, element_detail_value_text, source_system_code, dw_last_update_date_time)
    SELECT
        CURRENT_DATE('US/Central') AS file_date,
        stg.candidate_work_history_sid,
        stg.element_detail_entity_text,
        stg.element_detail_type_text,
        stg.element_detail_seq_num,
        stg.valid_from_date,
        stg.valid_to_date,
        stg.element_detail_id,
        stg.element_detail_value_text,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        (
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              'EMPLOYER ADDRESS' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") AS valid_to_date,
              CAST(NULL as INT64) AS element_detail_id,
              trim(concat(employeraddress_deliveryaddress_addressline1, employeraddress_deliveryaddress_addressline2, employeraddress_municipality, employeraddress_stateprovince, employeraddress_postalcode)) AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING))
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'
            WHERE length(trim(concat(employeraddress_deliveryaddress_addressline1, employeraddress_deliveryaddress_addressline2, employeraddress_municipality, employeraddress_stateprovince, employeraddress_postalcode))) > 0
             AND trim(concat(employeraddress_deliveryaddress_addressline1, employeraddress_deliveryaddress_addressline2, employeraddress_municipality, employeraddress_stateprovince, employeraddress_postalcode)) <> ''
             AND concat(employeraddress_deliveryaddress_addressline1, employeraddress_deliveryaddress_addressline2, employeraddress_municipality, employeraddress_stateprovince, employeraddress_postalcode) IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
          UNION ALL
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              'JOB_TITLE' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") AS valid_to_date,
              CAST(NULL as INT64) AS element_detail_id,
              trim(jobtitle) AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING))
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'
            WHERE length(trim(jobtitle)) > 0
             AND trim(jobtitle) <> ''
             AND trim(jobtitle) IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
          UNION ALL
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              'SALARY' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") AS valid_to_date,
              CAST(NULL as INT64) AS element_detail_id,
              trim(CAST(endpay as STRING)) AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING))
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'
            WHERE length(trim(CAST(endpay as STRING))) > 0
             AND trim(CAST(endpay as STRING)) <> ''
             AND trim(CAST(endpay as STRING)) IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
          UNION ALL
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              'REASON FOR LEAVING' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") as valid_to_date ,
              CAST(NULL as INT64) AS element_detail_id,
              trim(reasonforleaving) AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING))
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'
            WHERE length(trim(reasonforleaving)) > 0
             AND trim(reasonforleaving) <> ''
             AND trim(reasonforleaving) IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
          UNION ALL
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              r'MAY WE CONTACT THIS EMPLOYER?' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") as valid_to_date ,
              CAST(NULL as INT64) AS element_detail_id,
              CASE
                WHEN CASE
                   trim(CAST(permissiontocontact as STRING))
                  WHEN '' THEN 0.0
                  ELSE CAST(trim(CAST(permissiontocontact as STRING)) as FLOAT64)
                END = 1 THEN 'Y'
                WHEN CASE
                   trim(CAST(permissiontocontact as STRING))
                  WHEN '' THEN 0.0
                  ELSE CAST(trim(CAST(permissiontocontact as STRING)) as FLOAT64)
                END = 0 THEN 'N'
                ELSE 'N/A'
              END AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING))
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'
            WHERE length(trim(CAST(permissiontocontact as STRING))) > 0
             AND trim(CAST(permissiontocontact as STRING)) <> ''
             AND trim(CAST(permissiontocontact as STRING)) IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
          UNION ALL
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              'PROFESSIONAL REFERENCES' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") as valid_to_date ,
              CAST(NULL as INT64) AS element_detail_id,
              trim(employercontactname) AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING))
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'
            WHERE length(trim(employercontactname)) > 0
             AND trim(employercontactname) <> ''
             AND trim(employercontactname) IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
          UNION ALL
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              'WAGES' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") as valid_to_date ,
              CAST(NULL as INT64) AS element_detail_id,
              trim(CAST(endpay as STRING)) AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING))
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'
            WHERE length(trim(CAST(endpay as STRING))) > 0
             AND trim(CAST(endpay as STRING)) <> ''
             AND trim(CAST(endpay as STRING)) IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
          UNION ALL
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              'STATE' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") as valid_to_date ,
              CAST(NULL as INT64) AS element_detail_id,
              trim(employeraddress_stateprovince) AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING))
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'
            WHERE length(trim(employeraddress_stateprovince)) > 0
             AND trim(employeraddress_stateprovince) <> ''
             AND trim(employeraddress_stateprovince) IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
          UNION ALL
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              'CITY' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") as valid_to_date ,
              CAST(NULL as INT64) AS element_detail_id,
              trim(employeraddress_municipality) AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING))
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'
            WHERE length(trim(employeraddress_municipality)) > 0
             AND trim(employeraddress_municipality) <> ''
             AND trim(employeraddress_municipality) IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
          UNION ALL
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              'CONTACT_CURRENT_EMPLOYER' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") as valid_to_date ,
              CAST(NULL as INT64) AS element_detail_id,
              CASE
                WHEN CASE
                   CAST(permissiontocontact as STRING)
                  WHEN '' THEN 0.0
                  ELSE CAST(trim(CAST(permissiontocontact as STRING)) as FLOAT64)
                END = 1 THEN 'Y'
                WHEN CASE
                   trim(CAST(permissiontocontact as STRING))
                  WHEN '' THEN 0.0
                  ELSE CAST(trim(CAST(permissiontocontact as STRING)) as FLOAT64)
                END = 0 THEN 'N'
                ELSE 'N/A'
              END AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING))
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'
            WHERE length(trim(CAST(permissiontocontact as STRING))) > 0
             AND trim(CAST(permissiontocontact as STRING)) <> ''
             AND trim(CAST(permissiontocontact as STRING)) IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
          UNION ALL
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              'COUNTRY' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") as valid_to_date ,
              CAST(NULL as INT64) AS element_detail_id,
              trim(employeraddress_country) AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING))
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'
            WHERE length(trim(employeraddress_country)) > 0
             AND trim(employeraddress_country) <> ''
             AND trim(employeraddress_country) IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
          UNION ALL
          SELECT
              candidate_work_history_sid,
              'EXPERIENCE' AS element_detail_entity_text,
              'EMPLOYER_PHONE' AS element_detail_type_text,
              1 AS element_detail_seq_num,
              CURRENT_DATE('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") as valid_to_date ,
              CAST(NULL as INT64) AS element_detail_id,
              trim(employercontactphone_subscribernumber) AS element_detail_value_text,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              -- QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(CANDIDATE) ||TRIM(SEQUENCENUMBER) ORDER BY UPDATESTAMP DESC)=1
              {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON concat(trim(CAST(candidate as STRING)), trim(CAST(sequencenumber as STRING))) = trim(CAST(candidate_work_history_num as STRING)) 
               AND '9999-12-31' = '9999-12-31'
               AND upper(tgt.source_system_code) = 'B'  
            WHERE length(trim(employercontactphone_subscribernumber)) > 0
             AND trim(employercontactphone_subscribernumber) <> ''
             AND trim(employercontactphone_subscribernumber) IS NOT NULL  QUALIFY ROW_NUMBER() OVER (PARTITION BY trim(CAST(candidate as STRING)) ||trim(CAST(sequencenumber as STRING)) ORDER BY dw_last_update_date_time DESC)=1
        ) AS stg 

        
  ;

END;
