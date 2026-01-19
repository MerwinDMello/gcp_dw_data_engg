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
                   t3.mt_user_id AS mt_user_id,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
   FROM {{ params.param_im_auth_base_views_dataset_name }}.clinical_health_care_provider AS t1
   INNER JOIN
     (SELECT prctnr_role_idfn.role_plyr_sk,
             substr(trim(prctnr_role_idfn.id_txt), 1, 10) AS npi
      FROM {{ params.param_im_auth_base_views_dataset_name }}.prctnr_role_idfn
      WHERE upper(rtrim(prctnr_role_idfn.registn_type_ref_cd)) = 'NPI'
        AND prctnr_role_idfn.vld_to_ts = DATETIME '9999-12-31 00:00:00'
        AND rtrim(prctnr_role_idfn.id_txt) <> '' ) AS t2 ON COALESCE(CAST(t1.national_provider_id AS STRING), '') = upper(rtrim(t2.npi))
   INNER JOIN
     (SELECT prctnr_role_idfn.role_plyr_sk,
             mt_user_id AS mt_user_id
      FROM {{ params.param_im_auth_base_views_dataset_name }}.prctnr_role_idfn
      CROSS JOIN UNNEST(ARRAY[ substr(prctnr_role_idfn.id_txt, 10, 7) ]) AS mt_user_id
      WHERE upper(rtrim(prctnr_role_idfn.registn_type_ref_cd)) = 'LOGON_ID'
        AND length(trim(mt_user_id)) = 7
        AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(mt_user_id), 4, 4), '[A-Za-z_]') = 0
        AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(mt_user_id), 1, 3), '[0-9_]') = 0 ) AS t3 ON t2.role_plyr_sk = t3.role_plyr_sk
   WHERE t1.national_provider_id IS NOT NULL
     AND t1.national_provider_id > 0 QUALIFY row_number() OVER (PARTITION BY hcp_dw_id
                                                                ORDER BY upper(mt_user_id) DESC) = 1 ) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', coalesce(a.counts, 0)) AS source_string
FROM
  (SELECT count(*) AS counts
   FROM {{ params.param_im_stage_dataset_name }}.mt_cl_hcp_cdm) AS a)
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
