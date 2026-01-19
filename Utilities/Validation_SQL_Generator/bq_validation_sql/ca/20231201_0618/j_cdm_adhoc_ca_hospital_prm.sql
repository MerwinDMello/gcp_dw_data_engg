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
     (SELECT DISTINCT NULL AS hospital_sk,
                      ca.address_sk AS hospital_address_sk,
                      csrv.server_sk AS server_sk,
                      chs.hospitalid AS source_hospital_id,
                      chs.organizationid AS organization_id,
                      chs.hospname AS hospital_name,
                      chs.hospnpi AS hospital_npi_text,
                      chs.createdate AS source_create_date_time,
                      chs.lastupdate AS source_last_update_date_time,
                      chs.updatedby AS updated_by_3_4_id,
                      'C' AS source_system_code,
                      timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS chs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS csrv
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(chs.full_server_nm) = upper(csrv.server_name)
      LEFT OUTER JOIN {{ params.param_clinical_cdm_views_dataset_name }}.ref_ca_global_lookup AS cglus ON upper(coalesce(chs.hospcountry, ' ')) = upper(coalesce(cglus.sts_code_text, ' '))
      AND upper(cglus.short_name) = 'ISOCOUNTRY'
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_address AS ca
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(chs.hospaddress, ' ')) = upper(coalesce(ca.address_line_1_text, ' '))
      AND upper(coalesce(chs.hospaddress2, ' ')) = upper(coalesce(ca.address_line_2_text, ' '))
      AND upper(coalesce(chs.hospcity, ' ')) = upper(coalesce(ca.city_name, ' '))
      AND upper(coalesce(chs.hospstate, ' ')) = upper(coalesce(ca.state_name, ' '))
      AND upper(coalesce(chs.hospzip, ' ')) = upper(coalesce(ca.zip_code, ' '))
      AND coalesce(cglus.lookup_id, ' ') = coalesce(format('%11d', ca.country_id), ' ')
      AND ca.county_name IS NULL
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS ch
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ch.server_sk = csrv.server_sk
      AND ch.source_hospital_id = chs.hospitalid
      WHERE ch.server_sk IS NULL
        AND ch.source_hospital_id IS NULL ) AS a) AS b
);

SET
(tgt_rec_count) =
(
SELECT coalesce(count(*), 0)
FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital
WHERE ca_hospital.dw_last_update_date_time >=
    (SELECT max(etl_job_run.job_start_date_time) AS job_start_date_time
     FROM `hca-hin-dev-cur-clinical`.edwcdm_ac.etl_job_run
     WHERE upper(etl_job_run.job_name) = 'J_CDM_ADHOC_CA_HOSPITAL' )
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
