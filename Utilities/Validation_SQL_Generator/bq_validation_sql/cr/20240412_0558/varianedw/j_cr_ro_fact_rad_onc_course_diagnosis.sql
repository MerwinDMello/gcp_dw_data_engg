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

  SET srctablename = concat(srcdataset_id, '.', 'stg_factcoursediagnosis');

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
                             ORDER BY dp.dimsiteid,
                                      dp.factcoursediagnosisid DESC) AS fact_course_diagnosis_sk,
                            ra.fact_patient_diagnosis_sk AS fact_patient_diagnosis_sk,
                            ra1.patient_course_sk AS patient_course_sk,
                            dc.diagnosis_code_sk AS diagnosis_code_sk,
                            rr.site_sk AS site_sk,
                            dp.factcoursediagnosisid AS source_fact_course_diagnosis_id,
                            td_sysfnlib.decode(dp.isprimary, 1, 'Y', 0, 'N') AS primary_course_ind,
                            dp.logid AS log_id,
                            dp.runid AS run_id,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT stg_factcoursediagnosis.factcoursediagnosisid,
             stg_factcoursediagnosis.factpatientdiagnosisid,
             stg_factcoursediagnosis.dimcourseid,
             stg_factcoursediagnosis.dimdiagnosiscodeid,
             stg_factcoursediagnosis.dimsiteid,
             stg_factcoursediagnosis.isprimary,
             stg_factcoursediagnosis.logid,
             stg_factcoursediagnosis.runid
      FROM {{ params.param_cr_stage_dataset_name }}.stg_factcoursediagnosis) AS dp
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS rr ON rr.source_site_id = dp.dimsiteid
   LEFT OUTER JOIN
     (SELECT ra_0.source_fact_patient_diagnosis_id,
             ra_0.fact_patient_diagnosis_sk,
             rs.source_site_id
      FROM
        (SELECT fact_rad_onc_patient_diagnosis.source_fact_patient_diagnosis_id,
                fact_rad_onc_patient_diagnosis.fact_patient_diagnosis_sk,
                fact_rad_onc_patient_diagnosis.site_sk
         FROM {{ params.param_cr_core_dataset_name }}.fact_rad_onc_patient_diagnosis
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS ra_0
      INNER JOIN
        (SELECT ref_rad_onc_site.source_site_id,
                ref_rad_onc_site.site_sk
         FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS rs ON rs.site_sk = ra_0.site_sk) AS ra ON dp.factpatientdiagnosisid = ra.source_fact_patient_diagnosis_id
   AND ra.source_site_id = dp.dimsiteid
   LEFT OUTER JOIN
     (SELECT ra1_0.source_patient_course_id,
             ra1_0.patient_course_sk,
             rs1.source_site_id
      FROM
        (SELECT rad_onc_patient_course.source_patient_course_id,
                rad_onc_patient_course.patient_course_sk,
                rad_onc_patient_course.site_sk
         FROM {{ params.param_cr_core_dataset_name }}.rad_onc_patient_course
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS ra1_0
      INNER JOIN
        (SELECT ref_rad_onc_site.source_site_id,
                ref_rad_onc_site.site_sk
         FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS rs1 ON rs1.site_sk = ra1_0.site_sk) AS ra1 ON dp.dimcourseid = ra1.source_patient_course_id
   AND ra1.source_site_id = dp.dimsiteid
   LEFT OUTER JOIN
     (SELECT dc_0.source_diagnosis_code_id,
             dc_0.diagnosis_code_sk,
             rs2.source_site_id
      FROM
        (SELECT ref_rad_onc_diagnosis_code.source_diagnosis_code_id,
                ref_rad_onc_diagnosis_code.diagnosis_code_sk,
                ref_rad_onc_diagnosis_code.site_sk
         FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_diagnosis_code
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS dc_0
      INNER JOIN
        (SELECT ref_rad_onc_site.source_site_id,
                ref_rad_onc_site.site_sk
         FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS rs2 ON rs2.site_sk = dc_0.site_sk) AS dc ON dp.dimdiagnosiscodeid = dc.source_diagnosis_code_id
   AND dc.source_site_id = dp.dimsiteid) AS stg)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.fact_rad_onc_course_diagnosis
WHERE fact_rad_onc_course_diagnosis.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE)
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
