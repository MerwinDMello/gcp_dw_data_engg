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

  SET srctablename = concat(srcdataset_id, '.', 'cn_patient_imaging_stg');

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
  (SELECT stg.cn_patient_imaging_sid,
          stg.core_record_type_id,
          stg.nav_patient_id,
          stg.med_spcl_physician_id,
          stg.tumor_type_id,
          stg.diagnosis_result_id,
          stg.nav_diagnosis_id,
          stg.navigator_id,
          stg.coid,
          'H' AS company_code,
          stg.imaging_type_id,
          stg.imaging_date,
          rim.imaging_mode_id AS imaging_mode_id,
          rs.side_id AS imaging_area_side_id,
          stg.imaging_location_text,
          rf.facility_id AS imaging_facility_id,
          CASE
              WHEN upper(rtrim(stg.birad_scale_code)) = 'RESULTS NOT AVAILABLE' THEN CAST(NULL AS STRING)
              ELSE stg.birad_scale_code
          END AS birad_scale_code, stg.comment_text,
                                   ds.status_id AS disease_status_id,
                                   ts.status_id AS treatment_status_id,
                                   stg.other_image_type_text,
                                   CASE upper(rtrim(stg.initial_diagnosis_ind))
                                       WHEN 'YES' THEN 'Y'
                                       WHEN 'NO' THEN 'N'
                                       ELSE 'U'
                                   END AS initial_diagnosis_ind,
                                   CASE upper(rtrim(stg.disease_monitoring_ind))
                                       WHEN 'YES' THEN 'Y'
                                       WHEN 'NO' THEN 'N'
                                       ELSE 'U'
                                   END AS disease_monitoring_ind,
                                   stg.radiology_result_text,
                                   stg.hashbite_ssk,
                                   'N' AS source_system_code,
                                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_imaging_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_imaging_mode AS rim ON upper(rtrim(coalesce(trim(stg.imagemode), 'X'))) = upper(rtrim(coalesce(trim(rim.imaging_mode_desc), 'X')))
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_side AS rs ON upper(rtrim(coalesce(trim(stg.imagearea), 'XX'))) = upper(rtrim(coalesce(trim(rs.side_desc), 'XX')))
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_facility AS rf ON upper(rtrim(coalesce(trim(stg.imagecenter), 'XXX'))) = upper(rtrim(coalesce(trim(rf.facility_name), 'XXX')))
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_status AS ds ON upper(rtrim(coalesce(trim(stg.disease_status), 'XXX'))) = upper(rtrim(coalesce(trim(ds.status_desc), 'XXX')))
   AND upper(rtrim(ds.status_type_desc)) = 'DISEASE'
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_status AS ts ON upper(rtrim(coalesce(trim(stg.treatment_status), 'XXX'))) = upper(rtrim(coalesce(trim(ts.status_desc), 'XXX')))
   AND upper(rtrim(ts.status_type_desc)) = 'TREATMENT'
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_imaging.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_base_views_dataset_name }}.cn_patient_imaging) ) AS src)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cn_patient_imaging
WHERE cn_patient_imaging.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE)
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
