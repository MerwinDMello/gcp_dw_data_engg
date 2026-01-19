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

  SET sourcesysnm = 'navadhoc';

  SET srcdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_stage_dataset_name }}' , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.', 'cn_patient_procedure_pathology_result_stg');

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
                             ORDER BY stg.cn_patient_procedure_sid,
                                      rre.nav_result_id,
                                      upper(stg.navigation_procedure_type_code),
                                      stg.pathology_result_date,
                                      upper(stg.pathology_result_name),
                                      upper(stg.pathology_grade_available_ind),
                                      stg.pathology_grade_num,
                                      upper(stg.pathology_tumor_size_av_ind),
                                      upper(stg.tumor_size_num_text),
                                      upper(stg.margin_result_detail_text),
                                      upper(stg.sentinel_node_result_code),
                                      stg.estrogen_receptor_sw,
                                      upper(stg.estrogen_receptor_st_cd),
                                      upper(stg.estrogen_receptor_pct_text),
                                      stg.progesterone_receptor_sw,
                                      upper(stg.progesterone_receptor_st_cd),
                                      upper(stg.progesterone_receptor_pct_text),
                                      upper(stg.oncotype_diagnosis_score_num),
                                      upper(stg.oncotype_diagnosis_risk_text),
                                      upper(stg.comment_text),
                                      upper(stg.hashbite_ssk)) +
     (SELECT coalesce(max(cn_patient_procedure_pathology_result.cn_patient_proc_pathology_result_sid), 0) AS id1
      FROM {{ params.param_cr_core_dataset_name }}.cn_patient_procedure_pathology_result
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS cn_patient_proc_pathology_result_sid,
                            stg.cn_patient_procedure_sid AS cn_patient_procedure_sid,
                            rre.nav_result_id AS margin_result_id,
                            rre.nav_result_id AS nav_result_id,
                            rre.nav_result_id AS oncotype_diagnosis_result_id,
                            stg.navigation_procedure_type_code,
                            stg.pathology_result_date,
                            stg.pathology_result_name,
                            stg.pathology_grade_available_ind,
                            stg.pathology_grade_num,
                            stg.pathology_tumor_size_av_ind,
                            stg.tumor_size_num_text,
                            stg.margin_result_detail_text,
                            stg.sentinel_node_result_code,
                            stg.estrogen_receptor_sw,
                            stg.estrogen_receptor_st_cd,
                            stg.estrogen_receptor_pct_text,
                            stg.progesterone_receptor_sw,
                            stg.progesterone_receptor_st_cd,
                            stg.progesterone_receptor_pct_text,
                            stg.oncotype_diagnosis_score_num,
                            stg.oncotype_diagnosis_risk_text,
                            stg.comment_text,
                            stg.hashbite_ssk,
                            stg.source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_procedure_pathology_result_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rre ON upper(rtrim(stg.nav_result_id)) = upper(rtrim(rre.nav_result_desc))
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rre1 ON upper(rtrim(stg.margin_result_id)) = upper(rtrim(rre1.nav_result_desc))
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rre2 ON upper(rtrim(stg.oncotype_diagnosis_result_id)) = upper(rtrim(rre2.nav_result_desc))
   WHERE (upper(stg.hashbite_ssk),
          upper(stg.navigation_procedure_type_code)) NOT IN
       (SELECT AS STRUCT upper(cn_patient_procedure_pathology_result.hashbite_ssk) AS hashbite_ssk,
                         upper(cn_patient_procedure_pathology_result.navigation_procedure_type_code) AS navigation_procedure_type_code
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_procedure_pathology_result
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central'))
     AND DATE(stg.dw_last_update_date_time) = current_date('US/Central') ) AS src)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cn_patient_procedure_pathology_result
WHERE cn_patient_procedure_pathology_result.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE)
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
