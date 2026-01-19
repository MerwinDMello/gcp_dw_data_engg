##########################
## Variable Declaration ##
##########################

BEGIN

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;

DECLARE expected_value, actual_value, difference NUMERIC;

DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;

DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

DECLARE exp_values_list, act_values_list ARRAY<STRING>;

SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET srctableid = Null;

SET sourcesysnm = ''; -- This needs to be added

SET srcdataset_id = (select arr[offset(1)] from (select split("{{ params.param_pbs_stage_dataset_name }}" , '.') as arr));

SET srctablename = concat(srcdataset_id, '.','' ); -- This needs to be added

SET tgtdataset_id = (select arr[offset(1)] from (select split("{{ params.param_pbs_core_dataset_name }}" , '.') as arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 0;

SET exp_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT format('%20d', count(*) + 1) AS source_string
FROM
  (SELECT
     (SELECT coalesce(max(ref_remittance_rendering_provider.remittance_rendering_provider_sid), 0)
      FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_rendering_provider
      FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) + row_number() OVER (
                                                                                                ORDER BY upper(f.serv_provider_enty_type_qualifier_code),
                                                                                                         upper(f.rendering_provider_last_org_name),
                                                                                                         upper(f.rendering_provider_first_name),
                                                                                                         upper(f.rendering_provider_middle_name),
                                                                                                         upper(f.rendering_provider_name_suffix),
                                                                                                         upper(f.serv_provider_id_qualifier_code),
                                                                                                         upper(f.rendering_provider_id)) AS remittance_rendering_provider_sid, f.serv_provider_enty_type_qualifier_code,
                                                                                                                                                                               f.rendering_provider_last_org_name,
                                                                                                                                                                               f.rendering_provider_first_name,
                                                                                                                                                                               f.rendering_provider_middle_name,
                                                                                                                                                                               f.rendering_provider_name_suffix,
                                                                                                                                                                               f.serv_provider_id_qualifier_code,
                                                                                                                                                                               f.rendering_provider_id,
                                                                                                                                                                               'E' AS source_system_code,
                                                                                                                                                                               timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT max(remittance_claim.srvce_prvdr_enty_typ_qulfr_cod) AS serv_provider_enty_type_qualifier_code,
             max(remittance_claim.rendering_provider_last_org_nm) AS rendering_provider_last_org_name,
             max(remittance_claim.rendering_provider_first_name) AS rendering_provider_first_name,
             max(remittance_claim.rendering_provider_middle_name) AS rendering_provider_middle_name,
             max(remittance_claim.rendering_provider_name_suffix) AS rendering_provider_name_suffix,
             max(remittance_claim.srvce_prvdr_idntfctn_qulfr_cod) AS serv_provider_id_qualifier_code,
             max(remittance_claim.rendering_provider_id) AS rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE DATE(remittance_claim.dw_last_update_date_time) =
          (SELECT max(DATE(remittance_claim_0.dw_last_update_date_time))
           FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim AS remittance_claim_0)
        AND upper(coalesce(remittance_claim.srvce_prvdr_enty_typ_qulfr_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provider_last_org_nm, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provider_first_name, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provider_middle_name, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provider_name_suffix, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.srvce_prvdr_idntfctn_qulfr_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provider_id, '')) NOT IN('')
      GROUP BY upper(remittance_claim.srvce_prvdr_enty_typ_qulfr_cod),
               upper(remittance_claim.rendering_provider_last_org_nm),
               upper(remittance_claim.rendering_provider_first_name),
               upper(remittance_claim.rendering_provider_middle_name),
               upper(remittance_claim.rendering_provider_name_suffix),
               upper(remittance_claim.srvce_prvdr_idntfctn_qulfr_cod),
               upper(remittance_claim.rendering_provider_id)) AS f
   WHERE (upper(coalesce(f.serv_provider_enty_type_qualifier_code, '')),
          upper(coalesce(f.rendering_provider_last_org_name, '')),
          upper(coalesce(f.rendering_provider_first_name, '')),
          upper(coalesce(f.rendering_provider_middle_name, '')),
          upper(coalesce(f.rendering_provider_name_suffix, '')),
          upper(coalesce(f.serv_provider_id_qualifier_code, '')),
          upper(coalesce(f.rendering_provider_id, ''))) NOT IN
       (SELECT AS STRUCT upper(coalesce(ref_remittance_rendering_provider.serv_provider_enty_type_qualifier_code, '')),
                         upper(coalesce(ref_remittance_rendering_provider.rendering_provider_last_org_name, '')),
                         upper(coalesce(ref_remittance_rendering_provider.rendering_provider_first_name, '')),
                         upper(coalesce(ref_remittance_rendering_provider.rendering_provider_middle_name, '')),
                         upper(coalesce(ref_remittance_rendering_provider.rendering_provider_name_suffix, '')),
                         upper(coalesce(ref_remittance_rendering_provider.serv_provider_id_qualifier_code, '')),
                         upper(coalesce(ref_remittance_rendering_provider.rendering_provider_id, ''))
        FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_rendering_provider
        FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) ) AS a ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_rendering_provider
WHERE ref_remittance_rendering_provider.dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE ;)
);

SET idx_length = (SELECT ARRAY_LENGTH(act_values_list));

LOOP
  SET idx = idx + 1;

  IF idx > idx_length THEN
    BREAK;
  END IF;

  SET expected_value = CAST(exp_values_list[ORDINAL(idx)] AS NUMERIC);
  SET actual_value = CAST(act_values_list[ORDINAL(idx)] AS NUMERIC);

  SET difference = 
    CASE 
    WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64)
    WHEN expected_value = 0 and actual_value = 0 Then 0
    ELSE actual_value
    END;

  SET audit_status = 
  CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
  END;

  IF idx = 1 THEN
    SET audit_type = "RECORD_COUNT";
  ELSE
    SET audit_type = CONCAT("VALIDATION_CNTRLID_",idx);
  END IF;

  --Insert statement
  INSERT INTO "{{ params.param_pbs_audit_dataset_name }}".audit_control
  VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
  expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time, job_name, audit_time, audit_status
   );

END LOOP;
