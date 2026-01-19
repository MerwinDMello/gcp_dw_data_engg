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
  (SELECT DISTINCT b.*
   FROM
     (SELECT cac.contact_sk AS contact_sk,
             29 AS contact_detail_measure_id,
             cacs.aparticid AS contact_detail_measure_value_text,
             CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.aparticid IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            30 AS contact_detail_measure_id,
                            cacs.cparticid AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.cparticid IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            31 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.tparticid, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            32 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.mdnum, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            33 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.deanum, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            34 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.dear, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            35 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.dbcong AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.dbcong IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            36 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.dbadlt AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.dbadlt IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            37 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.dbthor AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.dbthor IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            38 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.presphy AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.presphy IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            39 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.rxpwd, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            40 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.surgid, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            41 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.surgnpi, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            42 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.tin, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            43 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.upin, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            44 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.contactidft, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            45 AS contact_detail_measure_id,
                            contact_detail_measure_value_text AS contact_detail_measure_value_text,
                            CAST(NULL AS INT64) AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      CROSS JOIN UNNEST(ARRAY[ substr(cacs.eclscenterid, 1, 5) ]) AS contact_detail_measure_value_text
      WHERE contact_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            46 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.providerid AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.providerid IS NOT NULL
      UNION DISTINCT SELECT cac.contact_sk AS contact_sk,
                            47 AS contact_detail_measure_id,
                            CAST(NULL AS STRING) AS contact_detail_measure_value_text,
                            cacs.accredidation AS contact_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cacs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS cac
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cac.server_sk
      AND cacs.contactid = cac.source_contact_id
      WHERE cacs.accredidation IS NOT NULL ) AS b
   WHERE upper(trim(b.contact_detail_measure_value_text)) <> ''
     OR b.contact_detail_measure_value_num IS NOT NULL ) AS c
);

SET
(tgt_rec_count) =
(
SELECT count(*)
FROM
  (SELECT ca_contact_detail.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact_detail
   WHERE ca_contact_detail.dw_last_update_date_time >=
       (SELECT max(etl_job_run.job_start_date_time)
        FROM `hca-hin-dev-cur-clinical`.edwcdm_dmx_ac.etl_job_run
        WHERE upper(etl_job_run.job_name) = upper('$JOBNAME')
          AND etl_job_run.job_start_date_time IS NOT NULL ) ) AS b
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
