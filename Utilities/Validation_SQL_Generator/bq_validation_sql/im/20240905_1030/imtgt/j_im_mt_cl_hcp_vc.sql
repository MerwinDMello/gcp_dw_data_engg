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

  SET srctablename = concat(srcdataset_id, '.', '');

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
  (SELECT DISTINCT t1.hcp_dw_id AS hcp_dw_id,
                   t2.hcp_user_id_3_4 AS hcp_user_id_3_4,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
   FROM {{ params.param_im_auth_base_views_dataset_name }}.clinical_health_care_provider AS t1
   INNER JOIN
     (SELECT DISTINCT t2_0.hcp_other_id AS hcp_user_id_3_4,
                      t1_0.hcp_npi
      FROM {{ params.param_im_base_views_dataset_name }}.hcp AS t1_0
      INNER JOIN {{ params.param_im_base_views_dataset_name }}.hcp_other_id AS t2_0 ON t1_0.hcp_dw_id = t2_0.hcp_dw_id
      AND t2_0.id_type_sid = 17248
      AND upper(rtrim(t2_0.hcp_other_id_active_ind)) = 'Y'
      AND length(trim(t2_0.hcp_other_id)) = 7
      AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(t2_0.hcp_other_id), 4, 4), '[A-Za-z_]') = 0
      AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(t2_0.hcp_other_id), 1, 3), '[0-9_]') = 0
      WHERE upper(rtrim(t1_0.hcp_active_ind)) = 'Y'
        AND NOT rtrim(t1_0.hcp_last_name) = ''
        AND NOT rtrim(t1_0.hcp_first_name) = ''
        AND rtrim(t2_0.hcp_other_id) <> '' QUALIFY row_number() OVER (PARTITION BY t1_0.hcp_npi
                                                                      ORDER BY upper(hcp_user_id_3_4)) = 1 ) AS t2 ON t1.national_provider_id = t2.hcp_npi
   WHERE NOT t1.national_provider_id IS NULL
     AND NOT t1.national_provider_id = 0.0 ) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM {{ params.param_im_stage_dataset_name }}.mt_cl_hcp_vc) AS a)
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
