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

  SET srctablename = concat(srcdataset_id, '.', 'cn_patient_stg');

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
  (SELECT stg.nav_patient_id,
          stg.navigator_id,
          CASE
              WHEN trim(stg.coid) IS NULL THEN '-1'
              ELSE stg.coid
          END AS coid,
          'H' AS company_code,
          stg.patient_market_urn,
          stg.medical_record_num,
          stg.empi_text,
          pd1.physician_id AS gynecologist_physician_id,
          pd2.physician_id AS primary_care_physician_id,
          cf.facility_mnemonic_cs AS facility_mnemonic_cs,
          cf.network_mnemonic_cs AS network_mnemonic_cs,
          stg.nav_create_date,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_physician_detail AS pd1
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(coalesce(trim(stg.gynecologist), 'X'))) = upper(rtrim(coalesce(trim(pd1.physician_name), 'X')))
   AND upper(rtrim(coalesce(trim(stg.gynecologistphone), 'X'))) = upper(rtrim(coalesce(trim(pd1.physician_phone_num), 'X')))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_physician_detail AS pd2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(coalesce(trim(stg.primarycarephysician), 'XX'))) = upper(rtrim(coalesce(trim(pd2.physician_name), 'XX')))
   AND upper(rtrim(coalesce(trim(stg.pcpphone), 'XX'))) = upper(rtrim(coalesce(trim(pd2.physician_phone_num), 'XX')))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility AS cf ON upper(rtrim(stg.coid)) = upper(rtrim(cf.coid))
   AND upper(rtrim(cf.facility_active_ind)) = 'Y'
   WHERE stg.nav_patient_id NOT IN
       (SELECT cn_patient.nav_patient_id
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) QUALIFY row_number() OVER (PARTITION BY stg.nav_patient_id
                                                                                                        ORDER BY primary_care_physician_id DESC) = 1 ) AS src)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cn_patient
WHERE cn_patient.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE)
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
