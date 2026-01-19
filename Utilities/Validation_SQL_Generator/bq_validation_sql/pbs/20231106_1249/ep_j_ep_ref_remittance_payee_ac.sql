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
     (SELECT coalesce(max(ref_remittance_payee.remittance_payee_sid), 0)
      FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_payee
      FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) + row_number() OVER (
                                                                                                ORDER BY provider_npi,
                                                                                                         stg.provider_tax_id,
                                                                                                         upper(max(stg.payee_name)),
                                                                                                         upper(max(stg.payee_identification_qual_code)),
                                                                                                         upper(max(stg.payee_city_name)),
                                                                                                         upper(max(stg.payee_state_code)),
                                                                                                         upper(max(stg.payee_postal_zone_code)),
                                                                                                         upper(max(stg.provider_tax_id_lookup_code))) AS remittance_payee_sid, provider_npi,
                                                                                                                                                                               CASE length(trim(format('%11d', stg.provider_tax_id)))
                                                                                                                                                                                   WHEN 9 THEN trim(format('%11d', stg.provider_tax_id))
                                                                                                                                                                                   WHEN 8 THEN concat('0', trim(format('%11d', stg.provider_tax_id)))
                                                                                                                                                                                   ELSE CAST(NULL AS STRING)
                                                                                                                                                                               END AS provider_tax_id,
                                                                                                                                                                               max(stg.payee_name) AS payee_name,
                                                                                                                                                                               max(stg.payee_identification_qual_code) AS payee_identification_qualifier_code,
                                                                                                                                                                               max(stg.payee_city_name) AS payee_city_name,
                                                                                                                                                                               max(stg.payee_state_code) AS payee_state_code,
                                                                                                                                                                               max(stg.payee_postal_zone_code) AS payee_postal_zone_code,
                                                                                                                                                                               'E' AS source_system_code,
                                                                                                                                                                               timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time,
                                                                                                                                                                               max(stg.provider_tax_id_lookup_code) AS provider_tax_id_lookup_code
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_payment AS stg
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN length(trim(format('%11d', stg.provider_tax_id))) = 10 THEN trim(format('%11d', stg.provider_tax_id))
                                ELSE CAST(NULL AS STRING)
                            END ]) AS provider_npi
   WHERE DATE(stg.dw_last_update_date_time) =
       (SELECT max(DATE(remittance_payment.dw_last_update_date_time))
        FROM {{ params.param_pbs_stage_dataset_name }}.remittance_payment)
     AND (upper(stg.payee_name),
          stg.payee_identification_qual_code,
          upper(stg.payee_city_name),
          upper(stg.payee_state_code),
          upper(stg.payee_postal_zone_code),
          upper(stg.provider_tax_id_lookup_code),
          CASE
              WHEN length(trim(format('%11d', stg.provider_tax_id))) = 10 THEN trim(format('%11d', stg.provider_tax_id))
              ELSE ''
          END,
          CASE length(trim(format('%11d', stg.provider_tax_id)))
              WHEN 9 THEN trim(format('%11d', stg.provider_tax_id))
              WHEN 8 THEN concat('0', trim(format('%11d', stg.provider_tax_id)))
              ELSE ''
          END) NOT IN
       (SELECT DISTINCT AS STRUCT upper(a.payee_name) AS payee_name,
                                  upper(a.payee_identification_qualifier_code) AS payee_identification_qualifier_code,
                                  upper(a.payee_city_name) AS payee_city_name,
                                  upper(a.payee_state_code) AS payee_state_code,
                                  upper(a.payee_postal_zone_code) AS payee_postal_zone_code,
                                  upper(a.provider_tax_id_lookup_code) AS provider_tax_id_lookup_code,
                                  coalesce(a.provider_npi, ''),
                                  coalesce(a.provider_tax_id, '')
        FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_payee AS a
        FOR system_time AS OF timestamp(tableload_start_time, 'US/Central'))
   GROUP BY 2,
            stg.provider_tax_id,
            upper(stg.payee_name),
            upper(stg.payee_identification_qual_code),
            upper(stg.payee_city_name),
            upper(stg.payee_state_code),
            upper(stg.payee_postal_zone_code),
            upper(stg.provider_tax_id_lookup_code)) AS a ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_payee
WHERE ref_remittance_payee.dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE ;)
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
