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
  (SELECT t1.im_domain_id,
          t1.mt_user_id,
          t1.esaf_activity_date,
          t1.access_rule_id,
          im_person_inactivate_sw AS im_person_inactivate_sw,
          'M' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT s1.im_domain_id,
             s1.mt_user_id,
             s1.esaf_activity_date,
             CASE
                 WHEN s1.mt_alias_exempt_sw = 1 THEN 0
                 WHEN s1.mt_excluded_user_sw = 1 THEN 0
                 WHEN s1.mt_staff_pm_user_sw = 1 THEN 0
                 WHEN s4.lawson_person_new_hire_sw = 1 THEN 0
                 WHEN s4.lawson_excluded_job_class_sw = 1 THEN 0
                 WHEN s4.lawson_excluded_department_sw = 1 THEN 0
                 WHEN s2.ad_user_new_sw = 1 THEN 0
                 WHEN s2.ad_group_exempt_sw = 1 THEN 0
                 WHEN s3.ad_employee_type_id = 41 THEN 0
                 WHEN s3.ad_employee_type_id = 16 THEN 0
                 WHEN s1.mt_user_activity_sw = 0 THEN 4
                 WHEN s2.ad_user_active_sw <> 1 THEN 5
                 WHEN s3.ad_account_user_id IS NULL THEN 6
                 ELSE 0
             END AS access_rule_id
      FROM {{ params.param_im_base_views_dataset_name }}.meditech_user_activity AS s1
      LEFT OUTER JOIN {{ params.param_im_base_views_dataset_name }}.ad_user_activity AS s2 ON upper(rtrim(s1.mt_user_id)) = upper(rtrim(s2.ad_account_user_id))
      AND s1.esaf_activity_date = s2.esaf_activity_date
      LEFT OUTER JOIN {{ params.param_im_base_views_dataset_name }}.ad_account AS s3 ON upper(rtrim(s1.mt_user_id)) = upper(rtrim(s3.ad_account_user_id))
      LEFT OUTER JOIN {{ params.param_im_base_views_dataset_name }}.lawson_user_activity AS s4 ON upper(rtrim(s1.mt_user_id)) = upper(rtrim(s4.lawson_person_user_id))
      AND s1.esaf_activity_date = s4.esaf_activity_date) AS t1
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN t1.access_rule_id = 0 THEN t1.access_rule_id
                                ELSE 1
                            END ]) AS im_person_inactivate_sw
   WHERE im_person_inactivate_sw = 1 ) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM {{ params.param_im_core_dataset_name }}.im_person_activity) AS a)
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
