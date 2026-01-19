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

  SET srctablename = concat(srcdataset_id, '.', 'patient_heme_disease_assessment_stg');

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
  (SELECT iq.*
   FROM
     (SELECT trim(phd.hbsource) AS hashbite_ssk
      FROM {{ params.param_cr_stage_dataset_name }}.patient_heme_disease_assessment_stg AS phd
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_test_type AS rt ON upper(trim(phd.testtype)) = upper(trim(rt.test_sub_type_desc))
      AND upper(trim(rt.test_type_desc)) = 'DISEASE ASSESSMENT'
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_sample_type AS rst ON upper(trim(phd.samplesourcetype)) = upper(trim(rst.sample_type_name))
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_disease_assess_source AS rda ON upper(trim(phd.source)) = upper(trim(rda.disease_assess_source_name))
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_facility AS rf ON upper(trim(phd.facilityname)) = upper(trim(rf.facility_name))
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_status AS rs ON upper(trim(phd.diseasestatus)) = upper(trim(rs.status_desc))
      AND upper(trim(rs.status_type_desc)) = 'DISEASE'
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_status AS rss ON upper(trim(phd.treatementstatus)) = upper(trim(rss.status_desc))
      AND upper(trim(rss.status_type_desc)) = 'TREATMENT') AS iq
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme_disease_assess AS cphd ON upper(trim(iq.hashbite_ssk)) = upper(trim(cphd.hashbite_ssk))
   WHERE trim(cphd.hashbite_ssk) IS NULL ) AS iqq)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme_disease_assess
WHERE cn_patient_heme_disease_assess.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE)
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
