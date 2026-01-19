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
FROM (SELECT concat(trim(format('%20d', count(x.unit_num_sid))), trim(CAST(sum(x.cash_receipt_amt) AS STRING)), trim(regexp_replace(format('%#17.2f', CAST(ROUND(sum(x.gross_revenue_amt), 2, 'ROUND_HALF_EVEN') AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3'))) AS source_string
FROM
  (SELECT max(z.unit_num_sid) AS unit_num_sid,
          z.patient_financial_class_sid,
          z.patient_type_sid,
          z.scenario_sid,
          max(z.time_sid) AS time_sid,
          z.source_sid,
          max(z.company_code) AS company_code,
          max(z.coid) AS coid,
          sum(z.cash_receipt_amt) AS cash_receipt_amt,
          sum(z.gross_revenue_amt) AS gross_revenue_amt
   FROM
     (SELECT max(sar.unit_num) AS unit_num_sid,
             CASE
                 WHEN sar.financial_class_code = 999 THEN 23
                 ELSE epf.patient_financial_class_sid
             END AS patient_financial_class_sid,
             1 AS scenario_sid,
             max(sar.rptg_period) AS time_sid,
             1 AS source_sid,
             CASE
                 WHEN upper(sar.derived_patient_type_code) = 'NA' THEN 40
                 ELSE pt.patient_type_sid
             END AS patient_type_sid,
             max(sar.company_code) AS company_code,
             max(sar.coid) AS coid,
             sum(sar.cash_amt) AS cash_receipt_amt,
             sum(NUMERIC '0.000') AS gross_revenue_amt
      FROM {{ params.param_auth_base_views_dataset_name }}.smry_ar_cash_collections AS sar
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.eis_patient_fin_class_dim AS epf ON CASE substr(epf.patient_financial_class_member, 5, 2)
                                                                                                              WHEN '' THEN 0.0
                                                                                                              ELSE CAST(substr(epf.patient_financial_class_member, 5, 2) AS FLOAT64)
                                                                                                          END = sar.financial_class_code
      AND upper(epf.patient_financial_class_member) <> 'NO_FC'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.eis_patient_type_dim AS pt ON upper(pt.patient_type_member) = upper(concat('PT_', CASE
                                                                                                                                                            WHEN upper(trim(sar.derived_patient_type_code)) = 'IE' THEN 'E'
                                                                                                                                                            ELSE sar.derived_patient_type_code
                                                                                                                                                        END))
      AND upper(substr(pt.patient_type_gen02, 1, 2)) <> 'MC'
      WHERE sar.payor_dw_id <> 999
        AND upper(sar.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
      GROUP BY upper(sar.unit_num),
               2,
               3,
               upper(sar.rptg_period),
               5,
               6,
               upper(sar.company_code),
               upper(sar.coid)
      UNION DISTINCT SELECT max(fd.unit_num) AS unit_num_sid,
                            epf.patient_financial_class_sid,
                            1 AS scenario_sid,
                            max(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))) AS time_sid,
                            1 AS source_sid,
                            CASE upper(rd.data_type_text)
                                WHEN 'IP AMOUNT' THEN 21
                                WHEN 'OP AMOUNT' THEN 27
                            END AS patient_type_sid,
                            max(fd.company_code) AS company_code,
                            max(rd.coid) AS coid,
                            sum(NUMERIC '0.000') AS cash_receipt_amt,
                            sum(rd.month_minus_1_amt) AS gross_revenue_amt
      FROM {{ params.param_auth_base_views_dataset_name }}.revstats_department AS rd
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_dimension AS fd ON upper(fd.coid) = upper(rd.coid)
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.eis_patient_fin_class_dim AS epf ON CASE substr(epf.patient_financial_class_member, 5, 2)
                                                                                                              WHEN '' THEN 0.0
                                                                                                              ELSE CAST(substr(epf.patient_financial_class_member, 5, 2) AS FLOAT64)
                                                                                                          END = rd.financial_class_code
      AND upper(epf.patient_financial_class_member) <> 'NO_FC'
      WHERE upper(rd.data_type_text) IN('IP AMOUNT',
                                        'OP AMOUNT')
        AND upper(rd.sub_unit_num) <> '00000'
        AND (upper(fd.company_code) = 'H'
             OR upper(substr(trim(fd.company_code_operations), 1, 1)) = 'Y')
      GROUP BY upper(fd.unit_num),
               2,
               3,
               upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))),
               5,
               6,
               upper(fd.company_code),
               upper(rd.coid)) AS z
   GROUP BY upper(z.unit_num_sid),
            2,
            3,
            4,
            upper(z.time_sid),
            6,
            upper(z.company_code),
            upper(z.coid)) AS x ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT concat(coalesce(trim(format('%20d', count(fact_rcom_ar_pat_fc_level.unit_num_sid))), '0'), coalesce(trim(CAST(sum(fact_rcom_ar_pat_fc_level.cash_receipt_amt) AS STRING)), '0'), coalesce(trim(CAST(sum(fact_rcom_ar_pat_fc_level.gross_revenue_amt) AS STRING)), '0')) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.fact_rcom_ar_pat_fc_level
WHERE fact_rcom_ar_pat_fc_level.time_sid = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                                               WHEN '' THEN 0.0
                                               ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS FLOAT64)
                                           END ;)
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
