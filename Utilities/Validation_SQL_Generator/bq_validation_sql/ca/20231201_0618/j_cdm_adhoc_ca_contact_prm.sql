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
     (SELECT DISTINCT NULL AS contact_sk,
                      a_0.contact_type_sk AS contact_type_sk,
                      addr.address_sk AS address_sk,
                      ser.server_sk AS server_sk,
                      stg.contactid AS source_contact_id,
                      stg.firstname AS contact_first_name,
                      stg.middlename AS contact_middle_name,
                      stg.lastname AS contact_last_name,
                      stg.suffix AS contact_suffix_name,
                      stg.emailname AS email_address_text,
                      stg.companyname AS company_name,
                      stg.notes AS note_text,
                      CAST(trim(stg.dateentered) AS DATETIME) AS contact_effective_from_date,
                      CAST(trim(stg.inactive) AS DATETIME) AS contact_effective_to_date,
                      stg.createdate AS source_create_date_time,
                      stg.lastupdate AS source_last_update_date_time,
                      stg.updatedby AS updated_by_3_4_id,
                      'C' AS source_system_code,
                      timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS stg
      LEFT OUTER JOIN
        (SELECT c.contact_type_sk,
                c.source_contact_type_id,
                s.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact_type AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON c.server_sk = s.server_sk) AS a_0 ON stg.contacttype = a_0.source_contact_type_id
      AND upper(stg.full_server_nm) = upper(a_0.server_name)
      LEFT OUTER JOIN {{ params.param_clinical_cdm_views_dataset_name }}.ref_ca_global_lookup AS cglus ON upper(stg.country) = upper(cglus.sts_code_text)
      AND upper(cglus.short_name) = 'ISOCOUNTRY'
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_address AS addr
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.address) = upper(addr.address_line_1_text)
      AND upper(stg.address2) = upper(addr.address_line_2_text)
      AND upper(stg.address3) = upper(addr.address_line_3_text)
      AND upper(stg.city) = upper(addr.city_name)
      AND upper(stg.stateorprovince) = upper(addr.state_name)
      AND upper(stg.postalcode) = upper(addr.zip_code)
      AND upper(stg.county) = upper(addr.county_name)
      AND cglus.lookup_id = addr.country_id
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS ser
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ser.server_name)
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS ch
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ch.server_sk = ser.server_sk
      AND ch.source_contact_id = stg.contactid
      WHERE ch.server_sk IS NULL
        AND ch.source_contact_id IS NULL ) AS a) AS b
);

SET
(tgt_rec_count) =
(
SELECT coalesce(count(*), 0)
FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact
WHERE ca_contact.dw_last_update_date_time >=
    (SELECT max(etl_job_run.job_start_date_time) AS job_start_date_time
     FROM `hca-hin-dev-cur-clinical`.edwcdm_ac.etl_job_run
     WHERE upper(etl_job_run.job_name) = 'J_CDM_ADHOC_CA_CONTACT' )
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
