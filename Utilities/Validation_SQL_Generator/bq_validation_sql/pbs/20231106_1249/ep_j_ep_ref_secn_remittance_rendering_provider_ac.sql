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
  (SELECT
     (SELECT coalesce(max(ref_secn_remittance_rendering_provider.remittance_secn_rendering_provider_sid), CAST(0 AS BIGNUMERIC))
      FROM {{ params.param_pbs_core_dataset_name }}.ref_secn_remittance_rendering_provider
      FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) + CAST(row_number() OVER (
                                                                                                     ORDER BY upper(f.secn_rendering_provider_id_qlfr_code), upper(f.secn_rendering_provider_id)) AS BIGNUMERIC) AS remittance_secn_rendering_provider_sid, f.secn_rendering_provider_id_qlfr_code,
                                                                                                                                                                                                                                                            f.secn_rendering_provider_id,
                                                                                                                                                                                                                                                            'E' AS source_system_code,
                                                                                                                                                                                                                                                            timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_1_cod AS secn_rendering_provider_id_qlfr_code,
             remittance_claim.rendering_provder_secndary_id1 AS secn_rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_1_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id1, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_2_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id2 AS secn_rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_2_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id2, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_3_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id3 AS secn_rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_3_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id3, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_4_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id4 AS secn_rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_4_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id4, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_5_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id5 AS secn_rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_5_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id5, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_6_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id6 AS secn_rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_6_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id6, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_7_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id7 AS secn_rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_7_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id7, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_8_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id8 AS secn_rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_8_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id8, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_9_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id9 AS secn_rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_9_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id9, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_rf_idn_qul_10_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provdr_secndary_id10 AS secn_rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_rf_idn_qul_10_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provdr_secndary_id10, '')) NOT IN('') ) AS f) AS a ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.ref_secn_remittance_rendering_provider
WHERE ref_secn_remittance_rendering_provider.dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE ;)
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
