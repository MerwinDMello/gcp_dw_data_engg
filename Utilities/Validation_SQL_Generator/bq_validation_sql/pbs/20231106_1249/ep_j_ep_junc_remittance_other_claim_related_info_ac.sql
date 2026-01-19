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
  (SELECT f.claim_guid,
          f.payment_guid,
          f.reference_id_line_num,
          rrap.ref_sid,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT remittance_claim.claim_guid,
             remittance_claim.payment_guid,
             1 AS reference_id_line_num,
             remittance_claim.othr_clm_rel_ref_idn_qual1_cod AS reference_id_qualifier_code,
             remittance_claim.other_claim_related_id1 AS reference_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual1_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id1, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            2 AS reference_id_line_num,
                            remittance_claim.othr_clm_rel_ref_idn_qual2_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id2 AS reference_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual2_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id2, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            3 AS reference_id_line_num,
                            remittance_claim.othr_clm_rel_ref_idn_qual3_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id3 AS reference_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual3_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id3, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            4 AS reference_id_line_num,
                            remittance_claim.othr_clm_rel_ref_idn_qual4_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id4 AS reference_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual4_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id4, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            5 AS reference_id_line_num,
                            remittance_claim.othr_clm_rel_ref_idn_qual5_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id5 AS reference_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual5_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id5, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            6 AS reference_id_line_num,
                            remittance_claim.othr_clm_rel_ref_idn_qual6_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id6 AS reference_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual6_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id6, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            7 AS reference_id_line_num,
                            remittance_claim.othr_clm_rel_ref_idn_qual7_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id7 AS reference_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual7_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id7, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            8 AS reference_id_line_num,
                            remittance_claim.othr_clm_rel_ref_idn_qual8_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id8 AS reference_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual8_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id8, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            9 AS reference_id_line_num,
                            remittance_claim.othr_clm_rel_ref_idn_qual9_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id9 AS reference_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual9_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id9, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            10 AS reference_id_line_num,
                            remittance_claim.othr_clm_rel_ref_idn_qul10_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id10 AS reference_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qul10_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id10, '')) NOT IN('') ) AS f
   LEFT OUTER JOIN {{ params.param_pbs_core_dataset_name }}.ref_remittance_other_claim_related_info AS rrap
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON upper(f.reference_id) = upper(rrap.reference_id)
   AND upper(f.reference_id_qualifier_code) = upper(rrap.reference_id_qualifier_code)) AS a ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.junc_remittance_other_claim_related_info
WHERE junc_remittance_other_claim_related_info.dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE ;)
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
