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
  (SELECT rs.service_guid AS service_guid,
          rs.claim_guid AS claim_guid,
          rs.audit_date AS audit_date,
          rs.delete_ind,
          rs.delete_date,
          coalesce(rc.coid, '') AS coid,
          coalesce(rc.company_code, '') AS company_code,
          rs.charge_amt AS charge_amt,
          rs.payment_amt AS payment_amt,
          rs.coinsurance_amt AS coinsurance_amt,
          rs.deductible_amt AS deductible_amt,
          rs.adjudicated_hcpcs_code AS adjudicated_hcpcs_code,
          rs.submitted_hcpcs_code AS submitted_hcpcs_code,
          rs.procedure_6_desc_7 AS submitted_hcpcs_code_desc,
          rs.revenue_code AS payor_sent_revenue_code,
          rs.adjudicated_hipps_code AS adjudicated_hipps_code,
          rs.submitted_hipps_code AS submitted_hipps_code,
          rs.apc_code AS apc_code,
          rs.apc_amt AS apc_amt,
          rs.quantity AS adjudicated_service_qty,
          rs.original_quantity_cnt AS submitted_service_qty,
          rs.hca_category AS service_category_code,
          rs.date_time_qualifier1 AS date_time_qualifier_code_1,
          parse_date('%Y/%m/%d', CASE
                                     WHEN length(rs.service_date1) > 0 THEN CASE
                                                                                WHEN length(SPLIT(rs.service_date1, '/')[ORDINAL(1)]) = 1
                                                                                     AND length(SPLIT(rs.service_date1, '/')[ORDINAL(2)]) = 1 THEN concat(SPLIT(rs.service_date1, '/')[ORDINAL(3)], '/0', SPLIT(rs.service_date1, '/')[ORDINAL(1)], '/0', SPLIT(rs.service_date1, '/')[ORDINAL(2)])
                                                                                WHEN length(SPLIT(rs.service_date1, '/')[ORDINAL(1)]) = 1 THEN concat(SPLIT(rs.service_date1, '/')[ORDINAL(3)], '/0', SPLIT(rs.service_date1, '/')[ORDINAL(1)], '/', SPLIT(rs.service_date1, '/')[ORDINAL(2)])
                                                                                WHEN length(SPLIT(rs.service_date1, '/')[ORDINAL(2)]) = 1 THEN concat(SPLIT(rs.service_date1, '/')[ORDINAL(3)], '/', SPLIT(rs.service_date1, '/')[ORDINAL(1)], '/0', SPLIT(rs.service_date1, '/')[ORDINAL(2)])
                                                                                ELSE concat(SPLIT(rs.service_date1, '/')[ORDINAL(3)], '/', SPLIT(rs.service_date1, '/')[ORDINAL(1)], '/', SPLIT(rs.service_date1, '/')[ORDINAL(2)])
                                                                            END
                                     ELSE CAST(NULL AS STRING)
                                 END) AS service_date_1, rs.date_time_qualifier2 AS date_time_qualifier_code_2,
                                                         parse_date('%Y/%m/%d', CASE
                                                                                    WHEN length(rs.service_date2) > 0 THEN CASE
                                                                                                                               WHEN length(SPLIT(rs.service_date2, '/')[ORDINAL(1)]) = 1
                                                                                                                                    AND length(SPLIT(rs.service_date2, '/')[ORDINAL(2)]) = 1 THEN concat(SPLIT(rs.service_date2, '/')[ORDINAL(3)], '/0', SPLIT(rs.service_date2, '/')[ORDINAL(1)], '/0', SPLIT(rs.service_date2, '/')[ORDINAL(2)])
                                                                                                                               WHEN length(SPLIT(rs.service_date2, '/')[ORDINAL(1)]) = 1 THEN concat(SPLIT(rs.service_date2, '/')[ORDINAL(3)], '/0', SPLIT(rs.service_date2, '/')[ORDINAL(1)], '/', SPLIT(rs.service_date2, '/')[ORDINAL(2)])
                                                                                                                               WHEN length(SPLIT(rs.service_date2, '/')[ORDINAL(2)]) = 1 THEN concat(SPLIT(rs.service_date2, '/')[ORDINAL(3)], '/', SPLIT(rs.service_date2, '/')[ORDINAL(1)], '/0', SPLIT(rs.service_date2, '/')[ORDINAL(2)])
                                                                                                                               ELSE concat(SPLIT(rs.service_date2, '/')[ORDINAL(3)], '/', SPLIT(rs.service_date2, '/')[ORDINAL(1)], '/', SPLIT(rs.service_date2, '/')[ORDINAL(2)])
                                                                                                                           END
                                                                                    ELSE CAST(NULL AS STRING)
                                                                                END) AS service_date_2, timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time,
                                                                                                        'E' AS source_system_code
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service AS rs
   LEFT OUTER JOIN {{ params.param_pbs_core_dataset_name }}.remittance_claim AS rc
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON upper(rc.claim_guid) = upper(rs.claim_guid)
   WHERE DATE(rs.dw_last_update_date_time) =
       (SELECT max(DATE(remittance_service.dw_last_update_date_time))
        FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service) ) AS a ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.remittance_service
WHERE DATE(remittance_service.dw_last_update_date_time) =
    (SELECT DATE(max(remittance_service_0.dw_last_update_date_time))
     FROM {{ params.param_pbs_core_dataset_name }}.remittance_service AS remittance_service_0) ;)
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
