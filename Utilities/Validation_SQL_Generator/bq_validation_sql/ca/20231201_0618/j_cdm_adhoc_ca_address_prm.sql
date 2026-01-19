##########################
## Variable Declaration ##
##########################

BEGIN

DECLARE difference, control2_difference, control3_difference, src_rec_count, tgt_rec_count int64 DEFAULT 0;

DECLARE control2_src_rowcount, control2_tgt_rowcount, control3_src_rowcount, control3_tgt_rowcount NUMERIC DEFAULT 0;

DECLARE srctableid, tolerance_percent INT64;

DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;

DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET srctableid = NULL;

SET sourcesysnm = ''; -- This needs to be added

SET srcdataset_id = (SELECT arr[offset(1)] FROM (SELECT SPLIT({{ params.param_ca_stage_dataset_name }} , '.') AS arr));

SET srctablename = concat(srcdataset_id, '.','' ); -- This needs to be added

SET tgtdataset_id = (SELECT arr[offset(1)] FROM (SELECT SPLIT({{ params.param_ca_core_dataset_name }} , '.') AS arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET audit_type ='RECORD_COUNT';

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 0;

SET
(src_rec_count) = 
(
SELECT count(*)
FROM
  (SELECT wrk1.*
   FROM
     (SELECT DISTINCT CASE
                          WHEN upper(trim(wrk1_0.address_line_1_text)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(wrk1_0.address_line_1_text)
                      END AS address_line_1_text,
                      CASE
                          WHEN upper(trim(wrk1_0.address_line_2_text)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(wrk1_0.address_line_2_text)
                      END AS address_line_2_text,
                      CASE
                          WHEN trim(format('%11d', wrk1_0.address_line_3_text)) = '' THEN CAST(NULL AS STRING)
                          ELSE substr(trim(format('%11d', wrk1_0.address_line_3_text)), 1, 100)
                      END AS address_line_3_text,
                      CASE
                          WHEN upper(trim(wrk1_0.city_name)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(wrk1_0.city_name)
                      END AS city_name,
                      CASE
                          WHEN upper(trim(wrk1_0.state_name)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(wrk1_0.state_name)
                      END AS state_name,
                      CASE
                          WHEN upper(trim(wrk1_0.zip_code)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(wrk1_0.zip_code)
                      END AS zip_code,
                      CASE
                          WHEN upper(trim(wrk1_0.county_name)) = '' THEN CAST(NULL AS STRING)
                          ELSE substr(trim(wrk1_0.county_name), 1, 50)
                      END AS county_name,
                      CASE
                          WHEN trim(format('%11d', wrk1_0.country_id)) = '' THEN CAST(NULL AS INT64)
                          ELSE CAST({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_td_normalize_number(trim(format('%11d', wrk1_0.country_id))) AS INT64)
                      END AS country_id
      FROM
        (SELECT DISTINCT cardioaccess_demographics_stg.pataddr AS address_line_1_text,
                         cardioaccess_demographics_stg.pataddr2 AS address_line_2_text,
                         CAST(NULL AS INT64) AS address_line_3_text,
                         substr(cardioaccess_demographics_stg.patcity, 1, 50) AS city_name,
                         cardioaccess_demographics_stg.patstate AS state_name,
                         cardioaccess_demographics_stg.patzip AS zip_code,
                         substr(cardioaccess_demographics_stg.county, 1, 50) AS county_name,
                         cardioaccess_demographics_stg.country AS country_id
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_demographics_stg
         UNION ALL SELECT DISTINCT CAST(NULL AS STRING) AS address_line_1_text,
                                   CAST(NULL AS STRING) AS address_line_2_text,
                                   CAST(NULL AS INT64) AS address_line_3_text,
                                   substr(cardioaccess_demographics_stg.birthcit, 1, 50) AS city_name,
                                   substr(cardioaccess_demographics_stg.birthsta, 1, 2) AS state_name,
                                   cardioaccess_demographics_stg.birthzip AS zip_code,
                                   CAST(NULL AS STRING) AS county_name,
                                   cardioaccess_demographics_stg.birthcou AS country_id
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_demographics_stg
         UNION ALL SELECT DISTINCT substr(cacs.address, 1, 150) AS address_line_1_text,
                                   substr(cacs.address2, 1, 60) AS address_line_2_text,
                                   CAST({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_td_normalize_number(cacs.address3) AS INT64) AS address_line_3_text,
                                   cacs.city AS city_name,
                                   substr(cacs.stateorprovince, 1, 2) AS state_name,
                                   substr(cacs.postalcode, 1, 15) AS zip_code,
                                   cacs.county AS county_name,

           (SELECT rcgl.lookup_id
            FROM {{ params.param_clinical_cdm_core_dataset_name }}.ref_ca_global_lookup AS rcgl
            FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
            WHERE upper(rcgl.sts_code_text) = upper(cacs.country)
              AND upper(rcgl.short_name) = 'ISOCOUNTRY' ) AS country_id
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
         UNION ALL SELECT DISTINCT substr(cahs.hospaddress, 1, 150) AS address_line_1_text,
                                   substr(cahs.hospaddress2, 1, 60) AS address_line_2_text,
                                   CAST(NULL AS INT64) AS address_line_3_text,
                                   cahs.hospcity AS city_name,
                                   cahs.hospstate AS state_name,
                                   substr(cahs.hospzip, 1, 15) AS zip_code,
                                   CAST(NULL AS STRING) AS county_name,

           (SELECT rcgl.lookup_id
            FROM {{ params.param_clinical_cdm_core_dataset_name }}.ref_ca_global_lookup AS rcgl
            FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
            WHERE upper(rcgl.sts_code_text) = upper(cahs.hospcountry)
              AND upper(rcgl.short_name) = 'ISOCOUNTRY' ) AS country_id
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs) AS wrk1_0
      WHERE CASE
                WHEN upper(trim(wrk1_0.address_line_1_text)) = '' THEN CAST(NULL AS STRING)
                ELSE trim(wrk1_0.address_line_1_text)
            END IS NOT NULL
        OR CASE
               WHEN upper(trim(wrk1_0.address_line_2_text)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(wrk1_0.address_line_2_text)
           END IS NOT NULL
        OR CASE
               WHEN trim(format('%11d', wrk1_0.address_line_3_text)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(format('%11d', wrk1_0.address_line_3_text))
           END IS NOT NULL
        OR CASE
               WHEN upper(trim(wrk1_0.city_name)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(wrk1_0.city_name)
           END IS NOT NULL
        OR CASE
               WHEN upper(trim(wrk1_0.state_name)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(wrk1_0.state_name)
           END IS NOT NULL
        OR CASE
               WHEN upper(trim(wrk1_0.zip_code)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(wrk1_0.zip_code)
           END IS NOT NULL
        OR CASE
               WHEN upper(trim(wrk1_0.county_name)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(wrk1_0.county_name)
           END IS NOT NULL
        OR CASE
               WHEN trim(format('%11d', wrk1_0.country_id)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(format('%11d', wrk1_0.country_id))
           END IS NOT NULL ) AS wrk1
   WHERE NOT EXISTS
       (SELECT 1
        FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_address AS caa
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
        WHERE upper(coalesce(wrk1.address_line_1_text, 'NULL')) = upper(coalesce(caa.address_line_1_text, 'NULL'))
          AND upper(coalesce(wrk1.address_line_2_text, 'NULL')) = upper(coalesce(caa.address_line_2_text, 'NULL'))
          AND upper(coalesce(wrk1.address_line_3_text, 'NULL')) = upper(coalesce(caa.address_line_3_text, 'NULL'))
          AND upper(coalesce(wrk1.city_name, 'NULL')) = upper(coalesce(caa.city_name, 'NULL'))
          AND upper(coalesce(wrk1.state_name, 'NULL')) = upper(coalesce(caa.state_name, 'NULL'))
          AND upper(coalesce(wrk1.zip_code, 'NULL')) = upper(coalesce(caa.zip_code, 'NULL'))
          AND upper(coalesce(wrk1.county_name, 'NULL')) = upper(coalesce(caa.county_name, 'NULL'))
          AND coalesce(format('%11d', wrk1.country_id), 'NULL') = coalesce(format('%11d', caa.country_id), 'NULL') ) ) AS a
);

SET
(tgt_rec_count) =
(
SELECT count(*)
FROM
  (SELECT ca_address.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_address
   WHERE ca_address.dw_last_update_date_time >=
       (SELECT max(etl_job_run.job_start_date_time)
        FROM `hca-hin-dev-cur-clinical`.edwcdm_dmx_ac.etl_job_run
        WHERE upper(etl_job_run.job_name) = upper('$JOBNAME')
          AND etl_job_run.job_start_date_time IS NOT NULL ) ) AS a
);

SET
difference = CASE 
              WHEN src_rec_count <> 0 Then CAST(((ABS(tgt_rec_count - src_rec_count)/src_rec_count) * 100) AS INT64)
              WHEN src_rec_count =0 AND tgt_rec_count = 0 Then 0
              ELSE tgt_rec_count
              END;


SET
audit_status = CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
END;

##Insert statement
INSERT INTO
 {{ params.param_ca_audit_dataset_name }}.audit_control
VALUES
  (GENERATE_UUID(), cast(srctableid AS int64), sourcesysnm, srctablename, tgttablename, audit_type, 
  src_rec_count, tgt_rec_count, control2_src_rowcount, control2_tgt_rowcount, control3_src_rowcount, control3_tgt_rowcount,
  cast(tableload_start_time AS DATETIME), cast(tableload_end_time AS DATETIME),
  tableload_run_time, job_name, audit_time, audit_status );
END;
