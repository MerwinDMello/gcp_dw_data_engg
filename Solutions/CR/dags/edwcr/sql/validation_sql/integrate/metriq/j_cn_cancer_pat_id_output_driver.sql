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

  SET sourcesysnm = 'metriq';

  SET srcdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_base_views_dataset_name }}' , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.', 'cancer_patient_id_output');

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
                             ORDER BY a1.cancer_patient_driver_sk,
                                      a1.cancer_patient_id_output_sk) AS cancer_patient_id_output_driver_sk,
                            a1.cancer_patient_driver_sk,
                            a1.cancer_patient_id_output_sk,
                            a1.message_control_id_text,
                            a1.user_action_date_time,
                            a1.coid,
                            a1.company_code,
                            a1.patient_dw_id,
                            a1.pat_acct_num,
                            a1.medical_record_num,
                            a1.patient_market_urn,
                            a1.last_name,
                            a1.first_name,
                            a1.birth_date,
                            a1.gender_code,
                            a1.social_security_num,
                            a1.address_line_1_text,
                            a1.address_line_2_text,
                            a1.city_name,
                            a1.state_code,
                            a1.zip_code,
                            a1.patient_type_code,
                            a1.message_type_code,
                            a1.message_event_type_code,
                            a1.message_flag_code,
                            a1.message_origin_requested_date_time,
                            a1.message_signed_observation_date_time,
                            a1.message_ingestion_date_time,
                            a1.message_created_date_time,
                            a1.document_type_id_text,
                            a1.document_type_text,
                            a1.document_type_coding_system_code,
                            a1.first_insert_date_time,
                            a1.icd_oncology_type_code,
                            a1.predicted_primary_icd_oncology_code,
                            a1.predicted_primary_icd_site_desc,
                            a1.suggested_primary_icd_oncology_code,
                            a1.suggested_primary_icd_site_desc,
                            a1.submitted_primary_icd_oncology_code,
                            a1.submitted_primary_icd_site_desc,
                            a1.transition_of_care_num,
                            a1.user_action_desc,
                            a1.user_action_criticality_text,
                            a1.user_3_4_id,
                            a1.report_assigned_date_time,
                            a1.attending_physician_full_name,
                            a1.pcp_full_name,
                            a1.pcp_phone_num,
                            a1.facility_menmonic_cs,
                            a1.network_mnemonic_cs,
                            a1.model_predicted_sarcoma_ind,
                            a1.submitted_sarcoma_ind,
                            a1.suggested_sarcoma_ind,
                            a1.source_system_code,
                            a1.dw_last_update_date_time
   FROM
     (SELECT DISTINCT cpd.cancer_patient_driver_sk,
                      a.cancer_patient_id_output_sk,
                      a.message_control_id_text,
                      a.user_action_date_time,
                      a.coid,
                      a.company_code,
                      a.patient_dw_id,
                      a.pat_acct_num,
                      a.medical_record_num,
                      a.patient_market_urn,
                      a.last_name,
                      a.first_name,
                      a.birth_date,
                      a.gender_code,
                      a.social_security_num,
                      a.address_line_1_text,
                      a.address_line_2_text,
                      a.city_name,
                      a.state_code,
                      a.zip_code,
                      a.patient_type_code,
                      a.message_type_code,
                      a.message_event_type_code,
                      a.message_flag_code,
                      a.message_origin_requested_date_time,
                      a.message_signed_observation_date_time,
                      a.message_ingestion_date_time,
                      a.message_created_date_time,
                      a.document_type_id_text,
                      a.document_type_text,
                      a.document_type_coding_system_code,
                      a.first_insert_date_time,
                      a.icd_oncology_type_code,
                      a.predicted_primary_icd_oncology_code,
                      a.predicted_primary_icd_site_desc,
                      a.suggested_primary_icd_oncology_code,
                      a.suggested_primary_icd_site_desc,
                      a.submitted_primary_icd_oncology_code,
                      a.submitted_primary_icd_site_desc,
                      a.transition_of_care_num,
                      a.user_action_desc,
                      a.user_action_criticality_text,
                      a.user_3_4_id,
                      a.report_assigned_date_time,
                      a.attending_physician_full_name,
                      a.pcp_full_name,
                      a.pcp_phone_num,
                      a.facility_menmonic_cs,
                      a.network_mnemonic_cs,
                      a.model_predicted_sarcoma_ind,
                      a.submitted_sarcoma_ind,
                      a.suggested_sarcoma_ind,
                      a.source_system_code,
                      a.dw_last_update_date_time
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output AS a
      INNER JOIN
        (SELECT cancer_patient_driver.cancer_patient_driver_sk,
                cancer_patient_driver.coid,
                cancer_patient_driver.cp_patient_id,
                cancer_patient_driver.network_mnemonic_cs
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver QUALIFY row_number() OVER (PARTITION BY cancer_patient_driver.cp_patient_id,
                                                                                                                          cancer_patient_driver.network_mnemonic_cs
                                                                                                             ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1) AS cpd ON a.patient_dw_id = cpd.cp_patient_id
      AND rtrim(a.network_mnemonic_cs) = rtrim(cpd.network_mnemonic_cs)
      UNION DISTINCT SELECT DISTINCT cpd.cancer_patient_driver_sk,
                                     a.cancer_patient_id_output_sk,
                                     a.message_control_id_text,
                                     a.user_action_date_time,
                                     coalesce(a.coid, df2.coid) AS coid,
                                     a.company_code,
                                     a.patient_dw_id,
                                     a.pat_acct_num,
                                     a.medical_record_num,
                                     a.patient_market_urn,
                                     a.last_name,
                                     a.first_name,
                                     a.birth_date,
                                     a.gender_code,
                                     a.social_security_num,
                                     a.address_line_1_text,
                                     a.address_line_2_text,
                                     a.city_name,
                                     a.state_code,
                                     a.zip_code,
                                     a.patient_type_code,
                                     a.message_type_code,
                                     a.message_event_type_code,
                                     a.message_flag_code,
                                     a.message_origin_requested_date_time,
                                     a.message_signed_observation_date_time,
                                     a.message_ingestion_date_time,
                                     a.message_created_date_time,
                                     a.document_type_id_text,
                                     a.document_type_text,
                                     a.document_type_coding_system_code,
                                     a.first_insert_date_time,
                                     a.icd_oncology_type_code,
                                     a.predicted_primary_icd_oncology_code,
                                     a.predicted_primary_icd_site_desc,
                                     a.suggested_primary_icd_oncology_code,
                                     a.suggested_primary_icd_site_desc,
                                     a.submitted_primary_icd_oncology_code,
                                     a.submitted_primary_icd_site_desc,
                                     a.transition_of_care_num,
                                     a.user_action_desc,
                                     a.user_action_criticality_text,
                                     a.user_3_4_id,
                                     a.report_assigned_date_time,
                                     a.attending_physician_full_name,
                                     a.pcp_full_name,
                                     a.pcp_phone_num,
                                     a.facility_menmonic_cs,
                                     a.network_mnemonic_cs,
                                     a.model_predicted_sarcoma_ind,
                                     a.submitted_sarcoma_ind,
                                     a.suggested_sarcoma_ind,
                                     a.source_system_code,
                                     a.dw_last_update_date_time
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output AS a
      LEFT OUTER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM {{ params.param_cr_base_views_dataset_name }}.clinical_facility QUALIFY row_number() OVER (PARTITION BY clinical_facility.facility_mnemonic_cs
                                                                                                         ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df2 ON rtrim(a.facility_menmonic_cs) = rtrim(df2.facility_mnemonic_cs)
      LEFT OUTER JOIN
        (SELECT cancer_patient_driver.cancer_patient_driver_sk,
                cancer_patient_driver.coid,
                coalesce(concat(cancer_patient_driver.patient_market_urn_text, cancer_patient_driver.network_mnemonic_cs), concat(cancer_patient_driver.medical_record_num, cancer_patient_driver.coid)) AS pid,
                cancer_patient_driver.network_mnemonic_cs
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver QUALIFY row_number() OVER (PARTITION BY pid
                                                                                                             ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1) AS cpd ON rtrim(coalesce(concat(a.patient_market_urn, a.network_mnemonic_cs), concat(a.medical_record_num, coalesce(a.coid, df2.coid)))) = rtrim(cpd.pid)
      AND rtrim(coalesce(a.network_mnemonic_cs, '-1')) = rtrim(coalesce(cpd.network_mnemonic_cs, '-1'))
      WHERE (coalesce(a.patient_dw_id, NUMERIC '0'),
             COALESCE(a.network_mnemonic_cs, '')) NOT IN
          (SELECT AS STRUCT coalesce(a_0.patient_dw_id, NUMERIC '0'),
                            COALESCE(a_0.network_mnemonic_cs,'')
           FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output AS a_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver AS cpd_0 ON a_0.patient_dw_id = cpd_0.cp_patient_id
           AND rtrim(a_0.network_mnemonic_cs) = rtrim(cpd_0.network_mnemonic_cs)) ) AS a1) AS stg)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_id_output_driver)
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
