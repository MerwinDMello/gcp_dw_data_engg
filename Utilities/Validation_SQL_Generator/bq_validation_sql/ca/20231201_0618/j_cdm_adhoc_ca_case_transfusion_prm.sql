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
     (SELECT NULL AS case_transfusion_sk,
             a_0.patient_case_sk AS patient_case_sk,
             trim(format('%11d', ca_server.server_sk)) AS server_sk,
             stg.transfusionid,
             stg.casenumber,
             stg.transfusion,
             stg.bldprodprbcdur,
             stg.bldprodffpdur,
             stg.bldprodfreshpdur,
             stg.bldprodsnglplatdur,
             stg.bldprodindplatdur,
             stg.bldprodcryodur,
             stg.bldprodfreshwbdur,
             stg.bldprodwbdur,
             stg.transfusbldprodlt24,
             stg.bldprodprbclt24,
             stg.bldprodffplt24,
             stg.bldprodfreshplt24,
             stg.bldprodsnglplatlt24,
             stg.bldprodindplatlt24,
             stg.bldprodcryolt24,
             stg.bldprodfreshwblt24,
             stg.bldprodwblt24,
             stg.transfusbldprodgt24,
             stg.bldprodprbcgt24,
             stg.bldprodffpgt24,
             stg.bldprodfreshpgt24,
             stg.bldprodsnglplatgt24,
             stg.bldprodindplatgt24,
             stg.bldprodcryogt24,
             stg.bldprodfreshwbgt24,
             stg.bldprodwbgt24,
             stg.dirdonorunits,
             stg.autologoustrans,
             stg.cellsavsal,
             stg.transfusbldprodany,
             stg.createdate,
             stg.lastupdate,
             trim(stg.updatedby) AS updatedby,
             stg.bldprodcryomlbef,
             stg.bldprodcryomlgt24,
             stg.bldprodcryomllt24,
             stg.bldprodffpmlbef,
             stg.bldprodffpmlgt24,
             stg.bldprodffpmllt24,
             stg.bldprodfreshpmlbef,
             stg.bldprodfreshpmlgt24,
             stg.bldprodfreshpmllt24,
             stg.bldprodfreshwbmlbef,
             stg.bldprodfreshwbmlgt24,
             stg.bldprodfreshwbmllt24,
             stg.bldprodplatmlbef,
             stg.bldprodplatmlgt24,
             stg.bldprodplatmllt24,
             stg.bldprodprbcmlbef,
             stg.bldprodprbcmlgt24,
             stg.bldprodprbcmllt24,
             stg.bldprodwbmlbef,
             stg.bldprodwbmlgt24,
             stg.bldprodwbmllt24,
             stg.transfusbldprodbefore,
             stg.cellsavsalml,
             trim(stg.server_name) AS SERVER_NAME,
             trim(stg.full_server_nm) AS full_server_nm,
             stg.dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_transfusion_stg AS stg
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(stg.full_server_nm)) = upper(trim(ca_server.server_name))
      LEFT OUTER JOIN
        (SELECT c.patient_case_sk,
                c.source_patient_case_num,
                s.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_case AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON c.server_sk = s.server_sk) AS a_0 ON stg.casenumber = a_0.source_patient_case_num
      AND upper(trim(stg.full_server_nm)) = upper(trim(a_0.server_name))) AS a) AS b
);

SET
(tgt_rec_count) =
(
SELECT count(*)
FROM
  (SELECT ca_case_transfusion.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_case_transfusion) AS a
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
