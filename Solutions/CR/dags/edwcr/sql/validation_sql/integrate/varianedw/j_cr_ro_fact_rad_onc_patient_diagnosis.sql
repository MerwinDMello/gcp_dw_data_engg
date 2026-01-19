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

  SET sourcesysnm = 'varianedw';

  SET srcdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_stage_dataset_name }}' , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.', 'stg_factpatientdiagnosis');

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
  (SELECT dp.fact_patient_sk,
          rpp.diagnosis_code_sk,
          rp.patient_sk,
          dp.diagnosis_status_id,
          dp.cell_category_id,
          dp.cell_grade_id,
          dp.laterality_id,
          dp.stage_id,
          dp.stage_status_id,
          dp.recurrence_id,
          dp.invasion_id,
          dp.confirmed_diagnosis_id,
          dp.diagnosis_type_id,
          rr.site_sk,
          dp.source_fact_patient_diagnosis_id,
          dp.diagnosis_status_date,
          dp.diagnosis_text,
          dp.clinical_text,
          dp.pathology_comment_text,
          dp.node_num,
          dp.positive_node_num,
          dp.log_id,
          dp.run_id,
          'R' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT row_number() OVER (
                                ORDER BY stg_factpatientdiagnosis.dimsiteid,
                                         stg_factpatientdiagnosis.factpatientdiagnosisid) AS fact_patient_sk,
                               stg_factpatientdiagnosis.dimlookupid_diagnosisstatus AS diagnosis_status_id,
                               stg_factpatientdiagnosis.dimlookupid_cellcategory AS cell_category_id,
                               stg_factpatientdiagnosis.dimlookupid_cellgrade AS cell_grade_id,
                               stg_factpatientdiagnosis.dimlookupid_laterality AS laterality_id,
                               stg_factpatientdiagnosis.dimlookupid_stage AS stage_id,
                               stg_factpatientdiagnosis.dimlookupid_stagestatus AS stage_status_id,
                               stg_factpatientdiagnosis.dimlookupid_recurrence AS recurrence_id,
                               stg_factpatientdiagnosis.dimlookupid_invasive AS invasion_id,
                               stg_factpatientdiagnosis.dimlookupid_confirmeddx AS confirmed_diagnosis_id,
                               stg_factpatientdiagnosis.dimlookupid_diagnosistype AS diagnosis_type_id,
                               stg_factpatientdiagnosis.factpatientdiagnosisid AS source_fact_patient_diagnosis_id,
                               DATE(CAST(trim(stg_factpatientdiagnosis.diagnosisstatusdate) AS DATETIME)) AS diagnosis_status_date,
                               stg_factpatientdiagnosis.diagnosisdescription AS diagnosis_text,
                               stg_factpatientdiagnosis.clinicaldescription AS clinical_text,
                               stg_factpatientdiagnosis.pathologycomments AS pathology_comment_text,
                               stg_factpatientdiagnosis.nodes AS node_num,
                               stg_factpatientdiagnosis.nodespositive AS positive_node_num,
                               stg_factpatientdiagnosis.logid AS log_id,
                               stg_factpatientdiagnosis.runid AS run_id,
                               stg_factpatientdiagnosis.dimdiagnosiscodeid,
                               stg_factpatientdiagnosis.dimpatientid,
                               stg_factpatientdiagnosis.dimsiteid
      FROM {{ params.param_cr_stage_dataset_name }}.stg_factpatientdiagnosis) AS dp
   INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_site AS rr ON dp.dimsiteid = rr.source_site_id
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_diagnosis_code AS rpp ON rpp.source_diagnosis_code_id = dp.dimdiagnosiscodeid
   AND rpp.site_sk = rr.site_sk
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient AS rp ON rp.source_patient_id = dp.dimpatientid
   AND rp.site_sk = rr.site_sk) AS stg)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.fact_rad_onc_patient_diagnosis
WHERE fact_rad_onc_patient_diagnosis.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE)
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
