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
  FROM (SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM
     (SELECT t4.im_domain_id,
             t1.pk_user_id_3_4 AS pk_user_id,
             t1.pk_database_instance_sid,
             DATE(t1.eff_from_date_time) AS pk_user_last_activity_date
      FROM {{ params.param_im_auth_base_views_dataset_name }}.pk_login_information AS t1
      INNER JOIN {{ params.param_im_auth_base_views_dataset_name }}.junc_pk_user_access_level AS t2 ON t1.pk_database_instance_sid = t2.pk_database_instance_sid
      AND t1.pk_person_id = t2.pk_person_id
      AND t2.pk_access_level_id = 3
      INNER JOIN {{ params.param_im_auth_base_views_dataset_name }}.ref_pk_data_base_instance AS t3 ON t1.pk_database_instance_sid = t3.pk_database_instance_sid
      INNER JOIN {{ params.param_im_base_views_dataset_name }}.ref_im_domain AS t4 ON t4.application_system_id = 8
      AND upper(rtrim(t3.pk_database_instance_code)) = upper(rtrim(t4.im_domain_name))
      WHERE length(trim(t1.pk_user_id_3_4)) = 7
        AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(t1.pk_user_id_3_4), 4, 4), '[A-Za-z_]') = 0
        AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(t1.pk_user_id_3_4), 1, 3), '[0-9_]') = 0
        AND DATE(t1.eff_from_date_time) >= date_sub(current_date('US/Central'), interval 365 DAY) QUALIFY row_number() OVER (PARTITION BY t1.pk_database_instance_sid,
                                                                                                                                          upper(pk_user_id)
                                                                                                                             ORDER BY t1.eff_from_date_time DESC) = 1 ) AS x) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM {{ params.param_im_core_dataset_name }}.pk_user) AS a)
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
      INSERT INTO {{ params.param_im_audit_dataset_name }}.audit_control
      VALUES
      (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
      expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
      tableload_run_time, job_name, audit_time, audit_status
      );

    END LOOP;

END;
