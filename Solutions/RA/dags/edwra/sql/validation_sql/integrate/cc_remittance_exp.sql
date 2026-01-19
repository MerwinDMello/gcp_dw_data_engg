##########################
## Variable Declaration ##
##########################

BEGIN

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;

DECLARE expected_value, actual_value, difference NUMERIC;

DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, audit_job_name, audit_status STRING;

DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

DECLARE exp_values_list, act_values_list ARRAY<STRING>;

SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET srctableid = Null;

SET sourcesysnm = 'ra'; -- This needs to be added

SET srcdataset_id = (select arr[offset(1)] from (select split("{{ params.param_parallon_ra_stage_dataset_name }}" , '.') as arr));

SET srctablename = concat(srcdataset_id, '.','ra_claim_payment' ); -- This needs to be added

SET tgtdataset_id = (select arr[offset(1)] from (select split("{{ params.param_parallon_ra_core_dataset_name }}" , '.') as arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET audit_job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 0;

SET exp_values_list = -- This needs to be added
(
SELECT [format('%20d', a.row_count)]
FROM
  (
select count(*) as row_count
FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat400amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id BETWEEN 400 AND 499
      GROUP BY 2,
               3) AS raaggcat400 ON racp.id = raaggcat400.ra_claim_payment_id
   AND racp.schema_id = raaggcat400.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat500amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id BETWEEN 500 AND 599
      GROUP BY 2,
               3) AS raaggcat500 ON racp.id = raaggcat500.ra_claim_payment_id
   AND racp.schema_id = raaggcat500.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat510amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id = 510
      GROUP BY 2,
               3) AS raaggcat510 ON racp.id = raaggcat510.ra_claim_payment_id
   AND racp.schema_id = raaggcat510.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat550amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id = 550
      GROUP BY 2,
               3) AS raaggcat550 ON racp.id = raaggcat550.ra_claim_payment_id
   AND racp.schema_id = raaggcat550.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_claim_supplement.supplement_amount, CAST(0 AS NUMERIC))) AS raclaimsupplementamt,
             ra_claim_supplement.ra_claim_payment_id,
             ra_claim_supplement.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_supplement
      WHERE upper(rtrim(ra_claim_supplement.supplement_type)) = 'ZM'
      GROUP BY 2,
               3) AS racs ON racp.id = racs.ra_claim_payment_id
   AND racp.schema_id = racs.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
   AND ragh.schema_id = racp.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
   AND racp.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_patient_type AS mpt ON mpt.schema_id = ma.schema_id
   AND mpt.id = ma.mon_patient_type_id
   AND mpt.org_id = ma.org_id_provider
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
   AND racp.schema_id = mpyr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
   AND pyr.schema_id = mpyr.schema_id
   LEFT OUTER JOIN
     (SELECT max(ra_service.procedure_code) AS procedure_code,
             ra_service.ra_claim_payment_id,
             ra_service.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_service
      WHERE upper(rtrim(ra_service.procedure_code_type)) = 'ZZ'
      GROUP BY 2,
               3,
               upper(ra_service.procedure_code)) AS rasvcode ON racp.id = rasvcode.ra_claim_payment_id
   AND racp.schema_id = rasvcode.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_identifier AS raci ON raci.ra_claim_payment_id = racp.id
   AND raci.schema_id = racp.schema_id
   AND upper(rtrim(raci.identifier_type)) = '9C'
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_identifier AS racice ON racice.ra_claim_payment_id = racp.id
   AND racice.schema_id = racp.schema_id
   AND upper(rtrim(racice.identifier_type)) = 'CE'
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = ma.account_no
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(raaggcat400.raaggcat400amount, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS ra_non_covered_charge_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(racp.ttl_claim_charge_amount - ROUND(ra_non_covered_charge_amt, 2, 'ROUND_HALF_EVEN'), CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS ra_net_billed_charge_amt
   CROSS JOIN UNNEST(ARRAY[ substr(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4), 1, 8) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN racp.date_created IS NOT NULL THEN racp.date_created
                                ELSE DATE '1800-01-01'
                            END ]) AS ra_log_date
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                ELSE og.client_id
                            END ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ])) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance
        WHERE cc_remittance.dw_last_update_date_time >=
   (SELECT coalesce(max(audit_control.load_end_time), date_add(timestamp_trunc(current_datetime('US/Central'), SECOND), INTERVAL -1 DAY))
	FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
	WHERE upper(audit_control.job_name) = upper(audit_job_name)
	  AND audit_control.load_end_time IS NOT NULL ) 
   ) AS a -- This needs to be added
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
    SET audit_type = CONCAT("INGEST_CNTRLID_",idx);
  END IF;

  --Insert statement
  INSERT INTO {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
  VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
  expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time, audit_job_name, audit_time, audit_status
   );

END LOOP;
END