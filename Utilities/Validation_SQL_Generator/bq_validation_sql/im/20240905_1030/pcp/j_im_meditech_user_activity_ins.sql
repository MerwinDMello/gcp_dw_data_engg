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

  SET sourcesysnm = 'pcp';

  SET srcdataset_id = (select arr[offset(1)] from (select split('{{ params.param_im_stage_dataset_name }}' , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.', 'staff_pm_users');

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
          current_date('US/Central') AS esaf_activity_date,
          t1.mt_user_last_activity_date,
          t1.mt_user_activity_sw,
          t1.mt_excluded_user_sw,
          t1.mt_staff_pm_user_sw,
          t1.mt_alias_exempt_sw,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT s1.im_domain_id,
                      s1.mt_user_id,
                      other_platform_activity_date AS other_platform_activity_date,
                      CASE
                          WHEN s1.mt_user_last_activity_date IS NULL THEN other_platform_activity_date
                          WHEN other_platform_activity_date IS NULL THEN s1.mt_user_last_activity_date
                          WHEN s1.mt_user_last_activity_date >= other_platform_activity_date THEN s1.mt_user_last_activity_date
                          ELSE other_platform_activity_date
                      END AS mt_user_last_activity_date,
                      CASE
                          WHEN s1.mt_user_last_activity_date IS NULL THEN 0
                          WHEN date_diff(current_date('US/Central'), s1.mt_user_last_activity_date, DAY) > 365 THEN 0
                          ELSE 1
                      END AS mt_user_activity_sw, CASE
                                                      WHEN rtrim(s1.mt_user_mnemonic_cs) IN('1PDSRB0154',
                                                                                            '1PDJRH1716',
                                                                                            '1PDMMW6006') THEN 1
                                                      ELSE 0
                                                  END AS mt_excluded_user_sw, CASE
                                                                                  WHEN s2.ntlogin IS NOT NULL THEN 1
                                                                                  ELSE 0
                                                                              END AS mt_staff_pm_user_sw, CASE
                                                                                                              WHEN s3.prctnr_mnem_cs IS NOT NULL THEN 1
                                                                                                              ELSE 0
                                                                                                          END AS mt_alias_exempt_sw
      FROM {{ params.param_im_base_views_dataset_name }}.meditech_user AS s1
      LEFT OUTER JOIN {{ params.param_im_stage_dataset_name }}.staff_pm_users AS s2 ON upper(rtrim(s1.mt_user_id)) = upper(rtrim(s2.ntlogin))
      LEFT OUTER JOIN
        (SELECT t1_0.im_domain_id,
                t2.ntwk_mnem_cs,
                t2.prctnr_mnem_cs,
                t2.prctnr_alias_nm
         FROM {{ params.param_im_base_views_dataset_name }}.ref_im_domain AS t1_0
         INNER JOIN
           (SELECT DISTINCT prctnr_actvt_dtl.ntwk_mnem_cs,
                            trim(prctnr_actvt_dtl.prctnr_mnem_cs) AS prctnr_mnem_cs,
                            prctnr_actvt_dtl.prctnr_alias_nm,
                            mt_user_exmpt AS mt_user_exmpt
            FROM {{ params.param_im_base_views_dataset_name }}.prctnr_actvt_dtl
            CROSS JOIN UNNEST(ARRAY[ CASE upper(rtrim(prctnr_actvt_dtl.prctnr_alias_nm))
                                         WHEN 'INTERFACE' THEN 1
                                         WHEN 'NON-HCA FACILITY' THEN 1
                                         WHEN 'OPO ORGAN PROCUREMENT' THEN 1
                                         WHEN 'OTHER' THEN 1
                                         WHEN 'OTHER – CPOE' THEN 1
                                         WHEN 'OTHER – DISASTER' THEN 1
                                         WHEN 'OTHER - DISASTER ID' THEN 1
                                         WHEN 'OTHER – DOWNTIME' THEN 1
                                         WHEN 'OTHER – ROBOT' THEN 1
                                         WHEN 'PAGER' THEN 1
                                         WHEN 'PAGER – PERSON' THEN 1
                                         WHEN 'PHONE' THEN 1
                                         WHEN 'SCRIPT' THEN 1
                                         WHEN 'TEMPLATE' THEN 1
                                         WHEN 'TEMPLATE - PARALLON SC' THEN 1
                                         WHEN 'TEMPLATE – PBPG' THEN 1
                                         WHEN 'TEMPLATE – PWS' THEN 1
                                         WHEN 'TRACKER' THEN 1
                                         WHEN 'USER' THEN 1
                                         WHEN 'USER – CORPORATE' THEN 1
                                         WHEN 'USER – EVS' THEN 1
                                         WHEN 'USER – FAN' THEN 1
                                         WHEN 'USER – PARALLON' THEN 1
                                         WHEN 'USER - PARALLON CODING' THEN 1
                                         WHEN 'USER - PARALLON HEALTHPORT' THEN 1
                                         WHEN 'USER - PARALLON TECHNOLOGY SOLUTIONS' THEN 1
                                         WHEN 'USER - PARALLON WORKFORCE SOLUTIONS' THEN 1
                                         WHEN 'USER – PWS' THEN 1
                                         WHEN 'USER - PWS LSC' THEN 1
                                         WHEN 'USER – SCRI, VENDOR' THEN 1
                                         ELSE 0
                                     END ]) AS mt_user_exmpt
            WHERE mt_user_exmpt = 1 ) AS t2 ON rtrim(t2.ntwk_mnem_cs) = rtrim(t1_0.im_domain_name)) AS s3 ON s1.im_domain_id = s3.im_domain_id
      AND rtrim(s1.mt_user_mnemonic_cs) = rtrim(s3.prctnr_mnem_cs)
      LEFT OUTER JOIN {{ params.param_im_base_views_dataset_name }}.pk_user AS s4 ON s1.im_domain_id = s4.im_domain_id
      AND upper(rtrim(s1.mt_user_id)) = upper(rtrim(s4.pk_user_id))
      LEFT OUTER JOIN
        (SELECT DISTINCT h2.mt_domain_id,
                         h1.hpf_user_id,
                         h1.hpf_user_last_activity_date
         FROM {{ params.param_im_base_views_dataset_name }}.hpf_account AS h1
         INNER JOIN {{ params.param_im_base_views_dataset_name }}.platform_domain_xwalk AS h2 ON h1.im_domain_id = h2.mt_domain_id) AS s5 ON upper(rtrim(s1.mt_user_id)) = upper(rtrim(s5.hpf_user_id))
      AND s1.im_domain_id = s5.mt_domain_id
      CROSS JOIN UNNEST(ARRAY[ CASE
                                   WHEN s5.hpf_user_last_activity_date IS NULL THEN s4.pk_user_last_activity_date
                                   WHEN s4.pk_user_last_activity_date IS NULL THEN s5.hpf_user_last_activity_date
                                   WHEN s5.hpf_user_last_activity_date >= s4.pk_user_last_activity_date THEN s5.hpf_user_last_activity_date
                                   ELSE s4.pk_user_last_activity_date
                               END ]) AS other_platform_activity_date) AS t1) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM {{ params.param_im_core_dataset_name }}.meditech_user_activity) AS a)
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
