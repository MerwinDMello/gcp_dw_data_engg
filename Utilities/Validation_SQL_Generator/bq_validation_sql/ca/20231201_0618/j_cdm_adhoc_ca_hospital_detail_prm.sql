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
  (SELECT DISTINCT a.*
   FROM
     (SELECT cah.hospital_sk AS hospital_sk,
             65 AS hospital_detail_measure_id,
             cahs.hospnumber AS hospital_detail_measure_value_text,
             CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.hospnumber IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            66 AS hospital_detail_measure_id,
                            cahs.contactfname AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.contactfname IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            67 AS hospital_detail_measure_id,
                            cahs.contactminit AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.contactminit IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            68 AS hospital_detail_measure_id,
                            cahs.contactlname AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.contactlname IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            69 AS hospital_detail_measure_id,
                            cahs.email AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.email IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            70 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.hosppfi, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            71 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.eclscenterid, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            72 AS hospital_detail_measure_id,
                            CAST(NULL AS STRING) AS hospital_detail_measure_value_text,
                            cahs.participantid AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.participantid IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            73 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.encryptionkey, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            74 AS hospital_detail_measure_id,
                            cahs.auxiliary0 AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.auxiliary0 IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            75 AS hospital_detail_measure_id,
                            cahs.hospguid AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.hospguid IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            76 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.pc4siteid, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            77 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.pc4encryptionkey, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            78 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.cparticipantid, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            79 AS hospital_detail_measure_id,
                            cahs.acchospnpi AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.acchospnpi IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            80 AS hospital_detail_measure_id,
                            cahs.pc4hospnpi AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.pc4hospnpi IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            81 AS hospital_detail_measure_id,
                            cahs.phonenumber AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.phonenumber IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            82 AS hospital_detail_measure_id,
                            cahs.faxnumber AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.faxnumber IS NOT NULL ) AS a
   WHERE upper(trim(a.hospital_detail_measure_value_text)) <> ''
     OR a.hospital_detail_measure_value_num IS NOT NULL ) AS b
);

SET
(tgt_rec_count) =
(
SELECT count(*)
FROM
  (SELECT ca_hospital_detail.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital_detail
   WHERE ca_hospital_detail.dw_last_update_date_time >=
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
