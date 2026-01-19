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
FROM (SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT remittance_service.claim_guid,
          'NA' AS payment_guid,
          remittance_service.service_guid,
          remittance_service.rendrng_provdr_ref_idn_qual1 AS secn_rendering_provider_id_qlfr_code,
          remittance_service.rendering_provider_identifier1 AS secn_rendering_provider_id,
          1 AS secn_rendering_provider_id_line_num
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual1, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier1, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual2 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier2 AS secn_rendering_provider_id,
                         2 AS secn_rendering_provider_id_line_num
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual2, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier2, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual3 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier3 AS secn_rendering_provider_id,
                         3 AS secn_rendering_provider_id_line_num
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual3, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier3, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual4 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier4 AS secn_rendering_provider_id,
                         4 AS secn_rendering_provider_id_line_num
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual4, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier4, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual5 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier5 AS secn_rendering_provider_id,
                         5 AS secn_rendering_provider_id_line_num
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual5, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier5, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual6 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier6 AS secn_rendering_provider_id,
                         6 AS secn_rendering_provider_id_line_num
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual6, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier6, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual7 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier7 AS secn_rendering_provider_id,
                         7 AS secn_rendering_provider_id_line_num
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual7, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier7, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual8 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier8 AS secn_rendering_provider_id,
                         8 AS secn_rendering_provider_id_line_num
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual8, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier8, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual9 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier9 AS secn_rendering_provider_id,
                         9 AS secn_rendering_provider_id_line_num
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual9, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier9, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual10 AS secn_rendering_provider_id_qlfr_code,
                         substr(remittance_service.rendering_providr_identifier10, 1, 100) AS secn_rendering_provider_id,
                         10 AS secn_rendering_provider_id_line_num
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual10, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_providr_identifier10, '')) NOT IN('') ) AS f
LEFT OUTER JOIN {{ params.param_pbs_core_dataset_name }}.ref_secn_remittance_rendering_provider AS rrap
FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON upper(f.secn_rendering_provider_id) = upper(rrap.secn_rendering_provider_id)
AND upper(f.secn_rendering_provider_id_qlfr_code) = upper(rrap.secn_rendering_provider_id_qlfr_code) ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.junc_remittance_secn_rendering_prov
WHERE junc_remittance_secn_rendering_prov.dw_last_update_date_time =
    (SELECT max(junc_remittance_secn_rendering_prov_0.dw_last_update_date_time)
     FROM {{ params.param_pbs_core_dataset_name }}.junc_remittance_secn_rendering_prov AS junc_remittance_secn_rendering_prov_0) ;)
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
