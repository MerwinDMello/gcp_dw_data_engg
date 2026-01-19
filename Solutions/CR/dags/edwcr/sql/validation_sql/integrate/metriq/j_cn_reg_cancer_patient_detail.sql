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

  SET srctablename = concat(srcdataset_id, '.', 'cr_patient');

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
  (
  SELECT
  DISTINCT met.cr_patient_id AS cr_patient_id,
  inav.nav_patient_id AS cn_patient_id,
  cpid.patient_dw_id AS cp_patient_id,
  COALESCE(met.coid, cpid.coid, inav.coid) AS coid,
  COALESCE(met.company_code, cpid.company_code, inav.company_code) AS company_code,
  COALESCE(met.network_mnemonic_cs, cpid.network_mnemonic_cs, inav.network_mnemonic_cs) AS network_mnemonic_cs,
  SUBSTR(COALESCE(met.patient_market_urn_text, cpid.patient_market_urn, inav.empi_text), 1, 30) AS patient_market_urn_text,
  COALESCE(met.medical_record_num, cpid.medical_record_num, inav.medical_record_num) AS medical_record_num,
  SUBSTR(met.relation_name, 1, 50) AS relationship_name,
  COALESCE(met.phone_num_type_code, inav.phone_num_type_code) AS phone_num_type_code,
  COALESCE(met.phone_num, inav.phone_num) AS phone_num,
  SUBSTR(met.insurance_type_name, 1, 50) AS insurance_type_name,
  met.preferred_contact_method_text,
  SUBSTR(met.insurance_company_name, 1, 50) AS insurance_company_name,
  COALESCE(met.source_system_code, cpid.source_system_code, inav.source_system_code) AS source_system_code
FROM (
  SELECT
    DISTINCT cp_0.patient_market_urn_text,
    cp_0.cr_patient_id,
    cp_0.coid,
    cp_0.company_code,
    cp_0.medical_record_num,
    rel.lookup_desc AS relation_name,
    df.network_mnemonic_cs,
    df.facility_mnemonic_cs,
    cppn.phone_num_type_code AS phone_num_type_code,
    cppn.phone_num AS phone_num,
    st.lookup_code AS state_code,
    ici.lookup_desc AS insurance_type_name,
    cpc.preferred_contact_method_text AS preferred_contact_method_text,
    iti.lookup_desc AS insurance_company_name,
    cp_0.source_system_code
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cr_patient AS cp_0
  INNER JOIN (
    SELECT
      clinical_facility.facility_mnemonic_cs,
      clinical_facility.network_mnemonic_cs,
      clinical_facility.coid,
      clinical_facility.facility_active_ind
    FROM
      {{ params.param_cr_auth_base_views_dataset_name }}.clinical_facility
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY UPPER(clinical_facility.coid)
      ORDER BY
        UPPER(clinical_facility.facility_active_ind) DESC) = 1) AS df
  ON
    UPPER(TRIM(cp_0.coid)) = UPPER(TRIM(df.coid))
  LEFT OUTER JOIN
    {{ params.param_cr_base_views_dataset_name }}.cr_patient_address AS cpa
  ON
    cp_0.cr_patient_id = cpa.cr_patient_id
  LEFT OUTER JOIN
    {{ params.param_cr_base_views_dataset_name }}.cr_patient_contact AS cpc
  ON
    cp_0.cr_patient_id = cpc.cr_patient_id
  LEFT OUTER JOIN
    {{ params.param_cr_base_views_dataset_name }}.cr_patient_phone_num AS cppn
  ON
    cp_0.cr_patient_id = cppn.cr_patient_id
    AND cpc.patient_contact_id = cppn.patient_contact_id
  LEFT OUTER JOIN
    {{ params.param_cr_base_views_dataset_name }}.cr_patient_insurance AS cpi
  ON
    cp_0.cr_patient_id = cpi.cr_patient_id
  LEFT OUTER JOIN (
    SELECT
      ref_lookup_code.master_lookup_sid,
      ref_lookup_code.lookup_desc
    FROM
      {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
    WHERE
      ref_lookup_code.lookup_id = 1 ) AS vs
  ON
    cp_0.vital_status_id = vs.master_lookup_sid
  LEFT OUTER JOIN (
    SELECT
      ref_lookup_code.master_lookup_sid,
      ref_lookup_code.lookup_desc
    FROM
      {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
    WHERE
      ref_lookup_code.lookup_id = 2 ) AS rc
  ON
    cp_0.patient_race_id = rc.master_lookup_sid
  LEFT OUTER JOIN (
    SELECT
      ref_lookup_code.master_lookup_sid,
      ref_lookup_code.lookup_desc
    FROM
      {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
    WHERE
      ref_lookup_code.lookup_id = 3 ) AS gn
  ON
    cp_0.patient_gender_id = gn.master_lookup_sid
  LEFT OUTER JOIN (
    SELECT
      ref_lookup_code.master_lookup_sid,
      ref_lookup_code.lookup_desc
    FROM
      {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
    WHERE
      ref_lookup_code.lookup_id = 4 ) AS rel
  ON
    cpc.contact_relation_id = rel.master_lookup_sid
  LEFT OUTER JOIN (
    SELECT
      ref_lookup_code.master_lookup_sid,
      ref_lookup_code.lookup_desc
    FROM
      {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
    WHERE
      ref_lookup_code.lookup_id = 13 ) AS iti
  ON
    cpi.insurance_type_id = iti.master_lookup_sid
  LEFT OUTER JOIN (
    SELECT
      ref_lookup_code.master_lookup_sid,
      ref_lookup_code.lookup_desc
    FROM
      {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
    WHERE
      ref_lookup_code.lookup_id = 14 ) AS ici
  ON
    cpi.insurance_company_id = ici.master_lookup_sid
  LEFT OUTER JOIN (
    SELECT
      ref_lookup_code.master_lookup_sid,
      ref_lookup_code.lookup_code,
      ref_lookup_code.lookup_desc
    FROM
      {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
    WHERE
      ref_lookup_code.lookup_id = 23 ) AS st
  ON
    cpa.state_id = st.master_lookup_sid) AS met
FULL OUTER JOIN (
  SELECT
    crio.patient_market_urn,
    crio.network_mnemonic_cs,
    crio.patient_dw_id,
    crio.coid,
    crio.medical_record_num,
    df1.facility_mnemonic_cs,
    crio.company_code,
    crio.source_system_code
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output AS crio
  INNER JOIN (
    SELECT
      clinical_facility.facility_mnemonic_cs,
      clinical_facility.network_mnemonic_cs,
      clinical_facility.coid,
      clinical_facility.facility_active_ind
    FROM
      {{ params.param_cr_auth_base_views_dataset_name }}.clinical_facility
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY UPPER(clinical_facility.coid)
      ORDER BY
        UPPER(clinical_facility.facility_active_ind) DESC) = 1) AS df1
  ON
    UPPER(TRIM(crio.coid)) = UPPER(TRIM(df1.coid))
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY COALESCE(CONCAT(crio.patient_market_urn, crio.network_mnemonic_cs), CONCAT(crio.medical_record_num, df1.facility_mnemonic_cs), FORMAT('%#20.0f', crio.patient_dw_id))
    ORDER BY
      crio.report_assigned_date_time DESC) = 1) AS cpid
ON
  TRIM(COALESCE(met.patient_market_urn_text, CONCAT(met.medical_record_num, met.facility_mnemonic_cs))) = TRIM(COALESCE(cpid.patient_market_urn, CONCAT(cpid.medical_record_num, cpid.facility_mnemonic_cs)))
  AND TRIM(met.network_mnemonic_cs) = TRIM(cpid.network_mnemonic_cs)
FULL OUTER JOIN (
  SELECT
    cp_0.nav_patient_id,
    cpt.patient_market_urn,
    df2.network_mnemonic_cs,
    cpt.coid,
    df2.facility_mnemonic_cs,
    cpt.company_code,
    cpt.empi_text,
    cpt.medical_record_num,
    cpp_0.phone_num,
    cpp_0.phone_num_type_code,
    cp_0.source_system_code
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cn_person AS cp_0
  LEFT OUTER JOIN
    {{ params.param_cr_base_views_dataset_name }}.cn_patient AS cpt
  ON
    cp_0.nav_patient_id = cpt.nav_patient_id
  INNER JOIN (
    SELECT
      clinical_facility.facility_mnemonic_cs,
      clinical_facility.network_mnemonic_cs,
      clinical_facility.coid,
      clinical_facility.facility_active_ind
    FROM
      {{ params.param_cr_auth_base_views_dataset_name }}.clinical_facility
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY UPPER(clinical_facility.coid)
      ORDER BY
        UPPER(clinical_facility.facility_active_ind) DESC) = 1) AS df2
  ON
    UPPER(TRIM(cpt.coid)) = UPPER(TRIM(df2.coid))
  LEFT OUTER JOIN
    {{ params.param_cr_base_views_dataset_name }}.cn_patient_address AS cpa
  ON
    cp_0.nav_patient_id = cpa.nav_patient_id
  LEFT OUTER JOIN
    {{ params.param_cr_base_views_dataset_name }}.cn_patient_phone_num AS cpp_0
  ON
    cp_0.nav_patient_id = cpp_0.nav_patient_id
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY COALESCE(cpt.patient_market_urn, CONCAT(cpt.medical_record_num, df2.facility_mnemonic_cs), FORMAT('%#20.0f', cp_0.nav_patient_id))
    ORDER BY
      cp_0.nav_patient_id DESC) = 1) AS inav
ON
  TRIM(COALESCE(CONCAT(TRIM(met.network_mnemonic_cs), met.patient_market_urn_text), CONCAT(met.medical_record_num, met.facility_mnemonic_cs))) = TRIM(COALESCE(inav.patient_market_urn, CONCAT(inav.medical_record_num, inav.facility_mnemonic_cs)))
  AND TRIM(met.network_mnemonic_cs) = TRIM(inav.network_mnemonic_cs)
LEFT OUTER JOIN (
  SELECT
    cancer_patient_driver.cr_patient_id,
    cancer_patient_driver.cancer_patient_driver_sk
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY cancer_patient_driver.cr_patient_id ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1) AS cp
ON
  met.cr_patient_id = cp.cr_patient_id
LEFT OUTER JOIN (
  SELECT
    cancer_patient_driver.cn_patient_id,
    cancer_patient_driver.cancer_patient_driver_sk
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY cancer_patient_driver.cn_patient_id ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1) AS cpn
ON
  inav.nav_patient_id = cpn.cn_patient_id
LEFT OUTER JOIN (
  SELECT
    cancer_patient_driver.cp_patient_id,
    cancer_patient_driver.cancer_patient_driver_sk
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY cancer_patient_driver.cp_patient_id ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1) AS cpp
ON
  cpid.patient_dw_id = cpp.cp_patient_id
  )
  )
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_detail)
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
