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

SET srctablename = concat(srcdataset_id, '.','cers_profile' ); -- This needs to be added

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
From (SELECT DISTINCT o.company_code,
                   o.coid,
                   cerp.id AS cers_profile_id,
                   trim(cerp.name) AS cers_profile_name,
                   cerp.date_created AS cers_profile_create_date,
                   cerp.date_updated AS cers_profile_update_date,
                   cerp.updated_by AS cers_profile_update_user_nm,
                   CASE
                       WHEN cerp.is_model = 1 THEN 'Y'
                       ELSE 'N'
                   END AS cers_model_ind,
                   cerp.ce_rs_category_id AS cers_category_id,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM {{ params.param_parallon_ra_stage_dataset_name }}.cers_profile AS cerp
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.cers_profile_establishment AS cpe ON cpe.cers_profile_id = cerp.id
   AND cpe.schema_id = cerp.schema_id
   INNER JOIN
     (SELECT seo.schema_id,
             seo.establishment_id,
             se.level_no,
             seo.org_id,
             se.id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.sec_establishment_org AS seo
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.sec_establishment AS se ON se.id = seo.establishment_id
      AND seo.schema_id = se.schema_id
      AND (se.level_no = 9
           OR se.level_no = 10)) AS estab ON estab.establishment_id = cpe.sec_establishment_id
   AND estab.schema_id = cpe.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS o ON estab.org_id = o.org_id
   AND estab.schema_id = o.schema_id)
) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(cers_profile_id) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_profile
        WHERE ref_cc_cers_profile.dw_last_update_date_time >=
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