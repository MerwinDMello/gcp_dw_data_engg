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
  (SELECT DISTINCT cap.patient_sk,
                   'R' AS address_type_code,
                   caa.address_sk
   FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_demographics_stg AS cads
   INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cads.full_server_nm) = upper(cas.server_name)
   INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient AS cap
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cads.patid = cap.source_patient_id
   AND cas.server_sk = cap.server_sk
   INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_address AS caa
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(cads.pataddr, 'NULL')) = upper(coalesce(caa.address_line_1_text, 'NULL'))
   AND upper(coalesce(cads.pataddr2, 'NULL')) = upper(coalesce(caa.address_line_2_text, 'NULL'))
   AND upper(coalesce(cads.patcity, 'NULL')) = upper(coalesce(caa.city_name, 'NULL'))
   AND upper(coalesce(cads.patstate, 'NULL')) = upper(coalesce(caa.state_name, 'NULL'))
   AND upper(coalesce(cads.patzip, 'NULL')) = upper(coalesce(caa.zip_code, 'NULL'))
   AND upper(coalesce(cads.county, 'NULL')) = upper(coalesce(caa.county_name, 'NULL'))
   AND coalesce(format('%11d', cads.country), 'NULL') = coalesce(format('%11d', caa.country_id), 'NULL')
   AND caa.address_line_3_text IS NULL
   UNION DISTINCT SELECT DISTINCT cap.patient_sk,
                                  'B' AS address_type_code,
                                  caa.address_sk
   FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_demographics_stg AS cads
   INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cads.full_server_nm) = upper(cas.server_name)
   INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient AS cap
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cads.patid = cap.source_patient_id
   AND cas.server_sk = cap.server_sk
   INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_address AS caa
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(cads.birthcit, 'NULL')) = upper(coalesce(caa.city_name, 'NULL'))
   AND upper(coalesce(cads.birthsta, 'NULL')) = upper(coalesce(caa.state_name, 'NULL'))
   AND upper(coalesce(cads.birthzip, 'NULL')) = upper(coalesce(caa.zip_code, 'NULL'))
   AND coalesce(format('%11d', cads.birthcou), 'NULL') = coalesce(format('%11d', caa.country_id), 'NULL')
   AND caa.address_line_1_text IS NULL
   AND caa.address_line_2_text IS NULL
   AND caa.address_line_3_text IS NULL
   AND caa.county_name IS NULL) AS b
);

SET
(tgt_rec_count) =
(
SELECT count(*)
FROM
  (SELECT ca_junc_patient_address.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_junc_patient_address
   WHERE ca_junc_patient_address.dw_last_update_date_time >=
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
