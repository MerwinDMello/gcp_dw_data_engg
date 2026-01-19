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
  (SELECT a.*
   FROM
     (SELECT NULL AS mortality_sk,
             a_0.patient_sk AS patient_sk,
             ca_server.server_sk AS server_sk,
             trim(format('%11d', stg.mtid)) AS source_mortality_id,
             CAST(trim(concat(substr(CAST(DATE(stg.mtdate) AS STRING), 1, 10), ' ', CAST(time(stg.mttime) AS STRING))) AS DATETIME) AS mortality_date_time,
             trim(format('%11d', stg.mtlocation)) AS mortality_location_id,
             trim(format('%11d', stg.mortalty)) AS mortality_hosp_id,
             trim(format('%11d', stg.primarycause)) AS primarycause,
             trim(format('%11d', stg.cardiac)) AS cardiac,
             trim(format('%11d', stg.renal)) AS renal,
             trim(format('%11d', stg.infection)) AS infection,
             trim(format('%11d', stg.valvular)) AS valvular,
             trim(format('%11d', stg.neurologic)) AS neurologic,
             trim(format('%11d', stg.vascular)) AS vascular,
             trim(format('%11d', stg.pulmonary)) AS pulmonary,
             trim(format('%11d', stg.other)) AS other,
             trim(format('%11d', stg.gi)) AS gi,
             trim(format('%11d', stg.prematurity)) AS prematurity,
             trim(format('%11d', stg.deathlab)) AS deathlab,
             trim(format('%11d', stg.autopsy)) AS autopsy,
             trim(stg.autopsydx) AS autopsydx,
             trim(stg.suspectedcausedeath) AS suspectedcausedeath,
             trim(format('%11d', stg.mtage)) AS mtage,
             trim(format('%11d', stg.deathcause)) AS deathcause,
             CAST(trim(CAST(stg.createdate AS STRING)) AS DATETIME) AS source_create_date_time,
             CAST(trim(CAST(stg.lastupdate AS STRING)) AS DATETIME) AS source_last_update_date_time,
             trim(stg.updateby) AS updated_by_3_4_id,
             'C' AS source_system_code,
             timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_mortality_stg AS stg
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ca_server.server_name)
      LEFT OUTER JOIN
        (SELECT c.patient_sk,
                c.source_patient_id,
                s.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON c.server_sk = s.server_sk) AS a_0 ON stg.patid = a_0.source_patient_id
      AND upper(stg.full_server_nm) = upper(a_0.server_name)) AS a) AS b
);

SET
(tgt_rec_count) =
(
SELECT count(*)
FROM
  (SELECT ca_mortality.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_mortality) AS a
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
