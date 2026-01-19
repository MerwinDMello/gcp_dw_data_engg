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

  SET sourcesysnm = 'varianedw';

  SET srcdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_stage_dataset_name }}' , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.', 'stg_dimprocedurecode');

  SET tgtdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_core_dataset_name }}' , '.') as arr));

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
  (SELECT row_number() OVER (
                             ORDER BY dp.dimsiteid,
                                      dp.dimprocedurecodeid) AS procedure_code_sk,
                            rtt.treatment_type_sk,
                            rr.site_sk,
                            dp.dimprocedurecodeid,
                            dp.procedurecode,
                            dp.description,
                            dp.procedurecodedescription,
                            dp.activeind,
                            dp.log_id,
                            dp.run_id,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(stg_dimprocedurecode.dimsiteid) AS INT64) AS dimsiteid,
             CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(stg_dimprocedurecode.dimprocedurecodeid) AS INT64) AS dimprocedurecodeid,
             stg_dimprocedurecode.procedurecode,
             stg_dimprocedurecode.description,
             stg_dimprocedurecode.procedurecodedescription,
             stg_dimprocedurecode.activeind,
             CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(stg_dimprocedurecode.logid) AS INT64) AS log_id,
             CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(stg_dimprocedurecode.runid) AS INT64) AS run_id
      FROM {{ params.param_cr_stage_dataset_name }}.stg_dimprocedurecode) AS dp
   LEFT OUTER JOIN
     (SELECT DISTINCT stg_sc_modalities.treatment_type,
                      stg_sc_modalities.treatment_category,
                      stg_sc_modalities.procedure_code
      FROM {{ params.param_cr_stage_dataset_name }}.stg_sc_modalities) AS sm ON upper(rtrim(sm.procedure_code)) = upper(rtrim(dp.procedurecode))
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_treatment_type AS rtt ON upper(rtrim(sm.treatment_category)) = upper(rtrim(rtt.treatment_category_desc))
   AND upper(rtrim(sm.treatment_type)) = upper(rtrim(rtt.treatment_type_desc))
   INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_site AS rr ON dp.dimsiteid = rr.source_site_id) AS stg)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_procedure_code
WHERE ref_rad_onc_procedure_code.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE)
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
      INSERT INTO {{ params.param_cr_audit_dataset_name }}.audit_control
      VALUES
      (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
      expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
      tableload_run_time, job_name, audit_time, audit_status
      );

    END LOOP;

END;
