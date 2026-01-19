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

  SET srctablename = concat(srcdataset_id, '.', 'hpf_instance_facility_xwalk');

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
  (SELECT t7.im_domain_id,
          cactus_provider_user_id AS cactus_provider_user_id,
          t1.hcp_src_sys_key,
          t1.hcp_npi,
          t6.fac_asgn_stts_sid,
          t6.fac_asgn_stts_src_sys_key,
          t2.prov_cat_sid,
          t2.prov_cat_src_sys_key,
          t1.hcp_first_name,
          t1.hcp_last_name,
          t1.hcp_middle_name,
          CASE
              WHEN rtrim(t6.fac_asgn_stts_desc) IN('Current',
                                                   'Temporary') THEN 1
              ELSE 0
          END AS cactus_provider_activity_exempt_sw,
          'K' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_im_base_views_dataset_name }}.hcp AS t1
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.ref_provider_category AS t2 ON t1.hcp_cat_sid = t2.prov_cat_sid
   AND upper(rtrim(t2.prov_cat_src_sys_key)) IN('D2G019AL2J',
                                                'D2G019AKNU')
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.hcp_other_id AS t3 ON t1.hcp_dw_id = t3.hcp_dw_id
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.ref_id_type AS t4 ON t3.id_type_sid = t4.id_type_sid
   AND upper(rtrim(t4.id_type_src_sys_key)) = 'D2QK0IXBT7'
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.hcp_facility_assignment AS t5 ON t1.hcp_dw_id = t5.hcp_dw_id
   AND upper(rtrim(t5.fac_asgn_active_ind)) = 'Y'
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.ref_facility_asgn_status AS t6 ON t5.fac_asgn_stts_sid = t6.fac_asgn_stts_sid
   AND t6.entity_sid = 1
   INNER JOIN {{ params.param_im_stage_dataset_name }}.hpf_instance_facility_xwalk AS t7 ON upper(rtrim(t5.coid)) = upper(rtrim(t7.coid))
   AND upper(rtrim(t5.company_code)) = upper(rtrim(t7.company_code))
   CROSS JOIN UNNEST(ARRAY[ substr(t3.hcp_other_id, 1, 7) ]) AS cactus_provider_user_id
   WHERE upper(rtrim(t1.hcp_active_ind)) = 'Y'
     AND cactus_provider_user_id IS NOT NULL
     AND NOT trim(cactus_provider_user_id) = ''
     AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(cactus_provider_user_id, '[.]') = 0
     AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(cactus_provider_user_id), 4, 4), '[A-Za-z_]') = 0
     AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(cactus_provider_user_id), 1, 3), '[0-9_]') = 0 QUALIFY row_number() OVER (PARTITION BY t7.im_domain_id,
                                                                                                                                                                            upper(cactus_provider_user_id)
                                                                                                                                                               ORDER BY t5.hcp_fac_asgn_eff_to_date DESC) = 1 ) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM {{ params.param_im_core_dataset_name }}.cactus_provider) AS a)
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
