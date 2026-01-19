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
     (SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                      cardioaccess_diagnosis_stg.full_server_nm,
                      210 AS diagnosis_detail_measure_id,
                      cardioaccess_diagnosis_stg.diagcateg AS diagnosis_detail_measure_value_text,
                      CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.diagcateg IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     211 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code1 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code1 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     212 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code2 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code2 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     213 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code3 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code3 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     214 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code4 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code4 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     215 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code5 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code5 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     216 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code6 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code6 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     217 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code7 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code7 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     218 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code8 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code8 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     219 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code9 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code9 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     220 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code10 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code10 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     221 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code11 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code11 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     222 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code12 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code12 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     223 AS diagnosis_detail_measure_id,
                                     diagnosis_detail_measure_value_text AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      CROSS JOIN UNNEST(ARRAY[ substr(cardioaccess_diagnosis_stg.olddiagname, 1, 55) ]) AS diagnosis_detail_measure_value_text
      WHERE diagnosis_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     224 AS diagnosis_detail_measure_id,
                                     diagnosis_detail_measure_value_text AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      CROSS JOIN UNNEST(ARRAY[ substr(cardioaccess_diagnosis_stg.class, 1, 55) ]) AS diagnosis_detail_measure_value_text
      WHERE diagnosis_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     225 AS diagnosis_detail_measure_id,
                                     CAST(NULL AS STRING) AS diagnosis_detail_measure_value_text,
                                     cardioaccess_diagnosis_stg.recur AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.recur IS NOT NULL ) AS a) AS b
);

SET
(tgt_rec_count) =
(
SELECT count(*)
FROM
  (SELECT ca_case_diagnosis_detail.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_case_diagnosis_detail
   WHERE ca_case_diagnosis_detail.dw_last_update_date_time >=
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
