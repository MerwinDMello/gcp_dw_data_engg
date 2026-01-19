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

  SET srcdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_stage_dataset_name }}' , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.', '');

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
                             ORDER BY met.cr_patient_id,
                                      inav.nav_patient_id,
                                      cpid.patient_dw_id) AS cancer_patient_driver_sk,
                            met.cr_patient_id AS cr_patient_id,
                            inav.nav_patient_id AS cn_patient_id,
                            cpid.patient_dw_id AS cp_patient_id,
                            coalesce(met.coid, cpid.coid, inav.coid) AS coid,
                            coalesce(met.company_code, cpid.company_code, inav.company_code) AS company_code,
                            coalesce(met.network_mnemonic_cs, cpid.network_mnemonic_cs, inav.network_mnemonic_cs) AS network_mnemonic_cs,
                            coalesce(met.patient_market_urn_text, cpid.patient_market_urn, inav.empi_text) AS patient_market_urn_text,
                            coalesce(met.medical_record_num, cpid.medical_record_num, inav.medical_record_num) AS medical_record_num,
                            empi.empi_text,
                            coalesce(met.patient_first_name, cpid.patient_first_name, inav.patient_first_name) AS patient_first_name,
                            coalesce(met.patient_middle_name, inav.patient_middle_name) AS patient_middle_name,
                            coalesce(met.patient_last_name, cpid.patient_last_name, inav.patient_last_name) AS patient_last_name,
                            inav.preferred_name,
                            coalesce(met.source_system_code, cpid.source_system_code, inav.source_system_code) AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT cp.patient_market_urn_text,
             cp.cr_patient_id,
             cp.coid,
             cp.company_code,
             cp.medical_record_num,
             df.facility_mnemonic_cs,
             df.network_mnemonic_cs,
             cp.patient_first_name AS patient_first_name,
             cp.patient_last_name AS patient_last_name,
             cp.patient_middle_name AS patient_middle_name,
             cp.source_system_code
      FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient AS cp
      INNER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                               ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df ON upper(rtrim(cp.coid)) = upper(rtrim(df.coid))) AS met
   FULL OUTER JOIN
     (SELECT crio.patient_market_urn,
             crio.network_mnemonic_cs,
             crio.medical_record_num,
             crio.patient_dw_id,
             crio.coid,
             crio.company_code,
             crio.first_name AS patient_first_name,
             crio.last_name AS patient_last_name,
             df1.facility_mnemonic_cs,
             crio.source_system_code
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output AS crio
      INNER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                               ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df1 ON upper(rtrim(crio.coid)) = upper(rtrim(df1.coid)) QUALIFY row_number() OVER (PARTITION BY coalesce(crio.patient_market_urn, concat(crio.medical_record_num, df1.facility_mnemonic_cs), format('%#20.0f', crio.patient_dw_id))
                                                                                                                                                                                                                                                      ORDER BY crio.report_assigned_date_time DESC) = 1) AS cpid ON rtrim(coalesce(met.patient_market_urn_text, concat(met.medical_record_num, met.facility_mnemonic_cs))) = rtrim(coalesce(cpid.patient_market_urn, concat(cpid.medical_record_num, cpid.facility_mnemonic_cs)))
   AND rtrim(met.network_mnemonic_cs) = rtrim(cpid.network_mnemonic_cs)
   FULL OUTER JOIN
     (SELECT cp.nav_patient_id,
             cpt.patient_market_urn,
             cpt.network_mnemonic_cs,
             cpt.medical_record_num,
             cpt.company_code,
             cpt.empi_text,
             cpt.coid,
             df2.facility_mnemonic_cs,
             cp.first_name AS patient_first_name,
             cp.last_name AS patient_last_name,
             cp.middle_name AS patient_middle_name,
             cp.preferred_name,
             cp.source_system_code
      FROM {{ params.param_cr_base_views_dataset_name }}.cn_person AS cp
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient AS cpt ON cp.nav_patient_id = cpt.nav_patient_id
      INNER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.network_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                               ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df2 ON upper(rtrim(cpt.coid)) = upper(rtrim(df2.coid))
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_address AS cpa ON cp.nav_patient_id = cpa.nav_patient_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_phone_num AS cpp ON cp.nav_patient_id = cpp.nav_patient_id QUALIFY row_number() OVER (PARTITION BY coalesce(cpt.patient_market_urn, concat(cpt.medical_record_num, df2.facility_mnemonic_cs), format('%#20.0f', cp.nav_patient_id))
                                                                                                                                                                     ORDER BY cp.nav_patient_id DESC) = 1) AS inav ON rtrim(coalesce(concat(trim(met.network_mnemonic_cs), met.patient_market_urn_text), concat(met.medical_record_num, met.facility_mnemonic_cs))) = rtrim(coalesce(inav.patient_market_urn, concat(inav.medical_record_num, inav.facility_mnemonic_cs)))
   AND rtrim(met.network_mnemonic_cs) = rtrim(inav.network_mnemonic_cs)
   LEFT OUTER JOIN
     (SELECT a.patient_market_urn,
             a.network_mnemonic_cs,
             a.empi_text
      FROM
        (SELECT cr.patient_market_urn,
                cr.network_mnemonic_cs,
                e.empi_text
         FROM {{ params.param_cr_views_dataset_name }}.clinical_registration AS cr
         INNER JOIN {{ params.param_cr_views_dataset_name }}.empi_encounter_id_xwalk AS e ON cr.patient_dw_id = e.encounter_id
         WHERE upper(rtrim(e.encounter_id_type_name)) = 'PATIENT_DW_ID'
           AND CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(e.empi_text) AS FLOAT64) > 0 ) AS a QUALIFY row_number() OVER (PARTITION BY upper(a.patient_market_urn),
                                                                                                                                                                     upper(a.network_mnemonic_cs)
                                                                                                                                                        ORDER BY upper(a.empi_text)) = 1) AS empi ON rtrim(coalesce(met.network_mnemonic_cs, cpid.network_mnemonic_cs, inav.network_mnemonic_cs)) = rtrim(empi.network_mnemonic_cs)
   AND upper(rtrim(coalesce(met.patient_market_urn_text, cpid.patient_market_urn, inav.empi_text))) = upper(rtrim(empi.patient_market_urn))) AS b)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver)
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
