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

  SET sourcesysnm = 'imtgt';

  SET srcdataset_id = (select arr[offset(1)] from (select split('{{ params.param_im_stage_dataset_name }}' , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.', 'mt_cl_hcp_cdm');

  SET tgtdataset_id = (select arr[offset(1)] from (select split('{{ params.param_im_core_dataset_name }}' , '.') as arr));

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
  FROM (SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT t1.company_code AS company_code,
          t1.coid AS coid,
          t1.hcp_mnemonic_cs AS hcp_mnemonic_cs,
          coalesce(t1.hcp_mis_user_mnemonic, '') AS hcp_mis_user_mnemonic,
          t1.network_mnemonic_cs AS network_mnemonic_cs,
          t1.national_provider_id AS national_provider_id,
          hcp_user_id_3_4 AS hcp_user_id_3_4,
          t1.hcp_full_name AS hcp_full_name,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
   FROM {{ params.param_im_auth_base_views_dataset_name }}.clinical_health_care_provider AS t1
   LEFT OUTER JOIN {{ params.param_im_stage_dataset_name }}.mt_cl_hcp_cdm AS t2 ON t1.hcp_dw_id = t2.hcp_dw_id
   LEFT OUTER JOIN {{ params.param_im_stage_dataset_name }}.mt_cl_hcp_vc AS t3 ON t1.hcp_dw_id = t3.hcp_dw_id
   CROSS JOIN UNNEST(ARRAY[ coalesce(t2.hcp_user_id, t3.hcp_user_id) ]) AS hcp_user_id_3_4
   WHERE NOT upper(rtrim(t1.hcp_mnemonic_cs)) = 'UNDEFINED'
     AND NOT hcp_user_id_3_4 IS NULL
     AND NOT rtrim(hcp_user_id_3_4) = '' QUALIFY row_number() OVER (PARTITION BY t1.hcp_dw_id,
                                                                                 hcp_mnemonic_cs
                                                                    ORDER BY t1.hcp_mis_user_mnemonic DESC, upper(hcp_user_id_3_4) DESC) = 1 ) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM {{ params.param_im_stage_dataset_name }}.mt_cl_hcp) AS a)
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
      INSERT INTO {{ params.param_im_audit_dataset_name }}.audit_control
      VALUES
      (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
      expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
      tableload_run_time, job_name, audit_time, audit_status
      );

    END LOOP;

END;
