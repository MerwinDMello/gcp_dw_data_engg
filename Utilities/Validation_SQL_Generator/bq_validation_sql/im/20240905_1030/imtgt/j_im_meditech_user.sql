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
  (SELECT DISTINCT t6.im_domain_id AS im_domain_id,
                   t4.mt_user_id,
                   substr(trim(t3.id_txt), 1, 10) AS national_provider_id,
                   t2.fclt_mnem_cd AS facility_mnemonic_cs,
                   t1.fl_nm AS mt_user_full_name,
                   substr(trim(t5.id_txt), 1, 10) AS mt_user_mnemonic_cs,
                   t1.prctnr_role_cd AS mt_user_page_1_provider_type_desc,
                   t2.prctnr_fclt_role_cd AS mt_user_page_2_provider_type_desc,
                   CASE
                       WHEN upper(rtrim(t1.actv_ind)) = 'Y' THEN CASE
                                                                     WHEN upper(rtrim(t2.prctnr_fclt_role_cd)) IN('1HCARESDNT',
                                                                                                                  '1HCAFELLOW',
                                                                                                                  '1HCASTUDEN',
                                                                                                                  '1HCAPHARM',
                                                                                                                  '2HCAACTIVE',
                                                                                                                  '2HCAASOCAF',
                                                                                                                  '2HCAAMBUL',
                                                                                                                  '2HCACOURTS',
                                                                                                                  '2HCACONSUL',
                                                                                                                  '2HCAPRVNOM',
                                                                                                                  '2HCANONPRV',
                                                                                                                  '2HCAHOLD') THEN 'Y'
                                                                     ELSE 'N'
                                                                 END
                       ELSE 'N'
                   END AS mt_user_exempt_sw,
                   t1.actv_ind AS mt_user_active_ind,
                   substr(trim(t7.id_txt), 1, 10) AS mt_user_mis_user_mnemonic,
                   t8.meditech_lst_logon_dt AS mt_user_last_activity_date,
                   'M' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_im_base_views_dataset_name }}.prctnr_dtl AS t1
   INNER JOIN
     (SELECT s1.role_plyr_sk,
             s1.vld_fr_ts,
             s1.company_code,
             s1.coid,
             s1.fclt_mnem_cd,
             s2.network_mnemonic_cs,
             s1.prctnr_fclt_role_cd
      FROM {{ params.param_im_base_views_dataset_name }}.prctnr_fclt_dtl AS s1
      INNER JOIN
        (SELECT clinical_facility.company_code,
                clinical_facility.coid,
                clinical_facility.network_mnemonic_cs
         FROM {{ params.param_im_auth_base_views_dataset_name }}.clinical_facility
         WHERE upper(rtrim(clinical_facility.company_code)) = 'H'
           AND NOT rtrim(clinical_facility.network_mnemonic_cs) = ''
           AND NOT trim(clinical_facility.network_mnemonic_cs) = '1SQI'
           AND NOT rtrim(clinical_facility.facility_mnemonic_cs) = ''
           AND NOT rtrim(substr(trim(clinical_facility.facility_mnemonic_cs), 6, 1)) = '_'
           AND NOT rtrim(substr(trim(clinical_facility.facility_mnemonic_cs), 7, 1)) = '_' ) AS s2 ON upper(rtrim(s1.company_code)) = upper(rtrim(s2.company_code))
      AND upper(rtrim(s1.coid)) = upper(rtrim(s2.coid))
      WHERE upper(rtrim(s1.company_code)) = 'H'
        AND s1.vld_to_ts = DATETIME '9999-12-31 00:00:00' ) AS t2 ON t1.role_plyr_sk = t2.role_plyr_sk
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.ref_im_domain AS t6 ON rtrim(t2.network_mnemonic_cs) = rtrim(t6.im_domain_name)
   AND t6.application_system_id IN(5,
                                   6)
   INNER JOIN
     (SELECT prctnr_role_idfn.role_plyr_sk,
             mt_user_id AS mt_user_id
      FROM {{ params.param_im_auth_base_views_dataset_name }}.prctnr_role_idfn
      CROSS JOIN UNNEST(ARRAY[ substr(prctnr_role_idfn.id_txt, 10, 7) ]) AS mt_user_id
      WHERE upper(rtrim(prctnr_role_idfn.registn_type_ref_cd)) = 'LOGON_ID'
        AND length(trim(mt_user_id)) = 7
        AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(mt_user_id, '[*.()_]') = 0
        AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(mt_user_id), 4, 4), '[A-Za-z_]') = 0
        AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(mt_user_id), 1, 3), '[0-9_]') = 0
        AND prctnr_role_idfn.vld_to_ts = DATETIME '9999-12-31 00:00:00' ) AS t4 ON t1.role_plyr_sk = t4.role_plyr_sk
   LEFT OUTER JOIN {{ params.param_im_base_views_dataset_name }}.prctnr_role_idfn AS t3 ON t1.role_plyr_sk = t3.role_plyr_sk
   AND upper(rtrim(t3.registn_type_ref_cd)) = 'NPI'
   AND t3.vld_to_ts <> DATETIME '9999-12-31 00:00:00'
   LEFT OUTER JOIN {{ params.param_im_base_views_dataset_name }}.prctnr_role_idfn AS t5 ON t1.role_plyr_sk = t5.role_plyr_sk
   AND upper(rtrim(t5.registn_type_ref_cd)) = 'MNEMONIC'
   AND t5.vld_to_ts = DATETIME '9999-12-31 00:00:00'
   AND rtrim(t5.id_txt) <> ''
   LEFT OUTER JOIN {{ params.param_im_base_views_dataset_name }}.prctnr_role_idfn AS t7 ON t1.role_plyr_sk = t7.role_plyr_sk
   AND upper(rtrim(t7.registn_type_ref_cd)) = 'USER_MNE'
   AND t7.vld_to_ts = DATETIME '9999-12-31 00:00:00'
   AND rtrim(t7.id_txt) <> ''
   LEFT OUTER JOIN {{ params.param_im_base_views_dataset_name }}.prctnr_actvt_dtl AS t8 ON t2.role_plyr_sk = t8.role_plyr_sk
   AND rtrim(t2.network_mnemonic_cs) = rtrim(t8.ntwk_mnem_cs)
   AND date_diff(current_date('US/Central'), t8.meditech_lst_logon_dt, DAY) <= 365
   AND t8.vld_to_ts = DATETIME '9999-12-31 00:00:00'
   WHERE upper(rtrim(t1.actv_ind)) = 'Y'
     AND t1.vld_to_ts = DATETIME '9999-12-31 00:00:00' QUALIFY row_number() OVER (PARTITION BY im_domain_id,
                                                                                               upper(t4.mt_user_id)
                                                                                  ORDER BY upper(mt_user_exempt_sw) DESC, mt_user_last_activity_date DESC) = 1 ) AS a ;

)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM {{ params.param_im_core_dataset_name }}.meditech_user) AS a)
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
