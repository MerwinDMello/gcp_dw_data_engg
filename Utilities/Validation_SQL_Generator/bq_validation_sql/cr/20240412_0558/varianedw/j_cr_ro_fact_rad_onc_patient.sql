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

  SET srctablename = concat(srcdataset_id, '.', 'stg_factpatient');

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
FROM {{ params.param_cr_stage_dataset_name }}.stg_factpatient)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY dhd.dimsiteid,
                                      dhd.factpatientid) AS fact_patient_sk,
                            rpp.hospital_sk AS hospital_sk,
                            rpa.patient_sk AS patient_sk,
                            dhd.patient_status_id AS patient_status_id,
                            dhd.location_sk,
                            dhd.race_id,
                            dhd.gender_id,
                            rr.site_sk AS site_sk,
                            dhd.factpatientid AS source_fact_patient_id,
                            dhd.creation_date_time,
                            dhd.admission_date_time,
                            dhd.discharge_date_time,
                            dhd.log_id,
                            dhd.run_id,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT stg_factpatient.dimsiteid AS dimsiteid,
                      stg_factpatient.factpatientid AS factpatientid,
                      stg_factpatient.dimhospitaldepartmentid,
                      stg_factpatient.dimpatientid,
                      stg_factpatient.dimlookupid_patientstatus AS patient_status_id,
                      stg_factpatient.dimlocationid AS location_sk,
                      stg_factpatient.dimlookupid_race AS race_id,
                      stg_factpatient.dimlookupid_gender AS gender_id,
                      CAST(trim(stg_factpatient.patientcreationdate) AS DATETIME) AS creation_date_time,
                      CAST(trim(stg_factpatient.patientadmissiondate) AS DATETIME) AS admission_date_time,
                      CAST(trim(stg_factpatient.patientdischargedate) AS DATETIME) AS discharge_date_time,
                      stg_factpatient.logid AS log_id,
                      stg_factpatient.runid AS run_id
      FROM {{ params.param_cr_stage_dataset_name }}.stg_factpatient) AS dhd
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_hospital AS rpp ON dhd.dimhospitaldepartmentid = rpp.source_hospital_id
   AND dhd.dimsiteid = rpp.site_sk
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient AS rpa ON dhd.dimpatientid = rpa.source_patient_id
   AND dhd.dimsiteid = rpa.site_sk
   INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_site AS rr ON rr.source_site_id = dhd.dimsiteid) AS ds
WHERE ds.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE)
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
