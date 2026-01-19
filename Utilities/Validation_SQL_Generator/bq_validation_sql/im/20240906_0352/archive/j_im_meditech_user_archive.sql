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

  SET sourcesysnm = 'archive';

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
  (SELECT t2.im_domain_id,
          t1.mt_user_id,
          t2.esaf_activity_date,
          t1.national_provider_id,
          t1.facility_mnemonic_cs,
          t1.mt_user_full_name,
          t1.mt_user_mnemonic_cs,
          t1.mt_user_page_1_provider_type_desc,
          t1.mt_user_page_2_provider_type_desc,
          t1.mt_user_exempt_sw,
          t1.mt_user_active_ind,
          t1.mt_user_mis_user_mnemonic,
          t1.mt_linked_user_code,
          t1.mt_user_last_activity_date,
          t1.source_system_code,
          t1.dw_last_update_date_time
   FROM {{ params.param_im_base_views_dataset_name }}.meditech_user AS t1
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.im_person_activity AS t2 ON upper(rtrim(t1.mt_user_id)) = upper(rtrim(t2.im_person_user_id))
   AND t1.im_domain_id = t2.im_domain_id
   UNION DISTINCT SELECT t3.im_domain_id,
                         t1.mt_user_id,
                         t3.esaf_activity_date,
                         t1.national_provider_id,
                         t1.facility_mnemonic_cs,
                         t1.mt_user_full_name,
                         t1.mt_user_mnemonic_cs,
                         t1.mt_user_page_1_provider_type_desc,
                         t1.mt_user_page_2_provider_type_desc,
                         t1.mt_user_exempt_sw,
                         t1.mt_user_active_ind,
                         t1.mt_user_mis_user_mnemonic,
                         t1.mt_linked_user_code,
                         t1.mt_user_last_activity_date,
                         t1.source_system_code,
                         t1.dw_last_update_date_time
   FROM {{ params.param_im_base_views_dataset_name }}.meditech_user AS t1
   INNER JOIN
     (SELECT DISTINCT h2.hpf_domain_id,
                      h2.mt_domain_id,
                      h1.mt_user_id
      FROM {{ params.param_im_base_views_dataset_name }}.meditech_user AS h1
      INNER JOIN {{ params.param_im_base_views_dataset_name }}.platform_domain_xwalk AS h2 ON h1.im_domain_id = h2.mt_domain_id) AS t2 ON t1.im_domain_id = t2.mt_domain_id
   AND upper(rtrim(t1.mt_user_id)) = upper(rtrim(t2.mt_user_id))
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.im_person_activity AS t3 ON upper(rtrim(t2.mt_user_id)) = upper(rtrim(t3.im_person_user_id))
   AND t2.hpf_domain_id = t3.im_domain_id QUALIFY row_number() OVER (PARTITION BY t3.im_domain_id,
                                                                                  upper(t1.mt_user_id),
                                                                                  t1.facility_mnemonic_cs
                                                                     ORDER BY t1.mt_user_exempt_sw,
                                                                              t1.mt_user_last_activity_date DESC) = 1) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM {{ params.param_im_core_dataset_name }}.meditech_user_activity_archive
   WHERE meditech_user_activity_archive.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE ) AS a)
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
