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
      COALESCE(met.company_code, cpid.company_code, inav.company_code) AS company_code,
      COALESCE(met.network_mnemonic_cs, cpid.network_mnemonic_cs, inav.network_mnemonic_cs) AS network_mnemonic_cs,
      SUBSTR(COALESCE(met.patient_market_urn_text, cpid.patient_market_urn, inav.empi_text), 1, 30) AS patient_market_urn_text,
      COALESCE(met.medical_record_num, cpid.medical_record_num, inav.medical_record_num) AS medical_record_num
    FROM (
      SELECT
        cp.patient_market_urn_text,
        cp.cr_patient_id,
        cp.coid,
        cp.company_code,
        cp.medical_record_num,
        df.facility_mnemonic_cs,
        df.network_mnemonic_cs
      FROM
        {{ params.param_cr_base_views_dataset_name }}.cr_patient AS cp
      LEFT OUTER JOIN (
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
        UPPER(RTRIM(cp.coid)) = UPPER(RTRIM(df.coid))) AS met
    FULL OUTER JOIN (
      SELECT
        crio.patient_market_urn,
        crio.network_mnemonic_cs,
        crio.medical_record_num,
        crio.patient_dw_id,
        COALESCE(crio.coid, df2.coid) AS coid,
        crio.company_code,
        COALESCE(df1.facility_mnemonic_cs, crio.facility_menmonic_cs) AS facility_mnemonic_cs,
        crio.source_system_code
      FROM
        {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output AS crio
      LEFT OUTER JOIN (
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
        UPPER(RTRIM(crio.coid)) = UPPER(RTRIM(df1.coid))
      LEFT OUTER JOIN (
        SELECT
          clinical_facility.facility_mnemonic_cs,
          clinical_facility.coid,
          clinical_facility.facility_active_ind
        FROM
          {{ params.param_cr_base_views_dataset_name }}.clinical_facility
        QUALIFY
          ROW_NUMBER() OVER (PARTITION BY clinical_facility.facility_mnemonic_cs ORDER BY UPPER(clinical_facility.facility_active_ind) DESC) = 1) AS df2
      ON
        RTRIM(crio.facility_menmonic_cs) = RTRIM(df2.facility_mnemonic_cs)
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY COALESCE(CONCAT(crio.patient_market_urn, crio.network_mnemonic_cs), CONCAT(crio.medical_record_num, facility_mnemonic_cs), FORMAT('%#20.0f', crio.patient_dw_id))
        ORDER BY
          crio.report_assigned_date_time DESC) = 1) AS cpid
    ON
      RTRIM(COALESCE(met.patient_market_urn_text, CONCAT(met.medical_record_num, met.facility_mnemonic_cs))) = RTRIM(COALESCE(cpid.patient_market_urn, CONCAT(cpid.medical_record_num, cpid.facility_mnemonic_cs)))
      AND RTRIM(met.network_mnemonic_cs) = RTRIM(cpid.network_mnemonic_cs)
    FULL OUTER JOIN (
      SELECT
        cp.nav_patient_id,
        cpt.patient_market_urn,
        cpt.network_mnemonic_cs,
        cpt.medical_record_num,
        cpt.company_code,
        cpt.empi_text,
        cpt.coid,
        df2.facility_mnemonic_cs
      FROM
        {{ params.param_cr_base_views_dataset_name }}.cn_person AS cp
      LEFT OUTER JOIN
        {{ params.param_cr_base_views_dataset_name }}.cn_patient AS cpt
      ON
        cp.nav_patient_id = cpt.nav_patient_id
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
        UPPER(RTRIM(cpt.coid)) = UPPER(RTRIM(df2.coid))
      LEFT OUTER JOIN
        {{ params.param_cr_base_views_dataset_name }}.cn_patient_address AS cpa
      ON
        cp.nav_patient_id = cpa.nav_patient_id
      LEFT OUTER JOIN
        {{ params.param_cr_base_views_dataset_name }}.cn_patient_phone_num AS cpp
      ON
        cp.nav_patient_id = cpp.nav_patient_id
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY COALESCE(cpt.patient_market_urn, CONCAT(cpt.medical_record_num, df2.facility_mnemonic_cs), FORMAT('%#20.0f', cp.nav_patient_id))
        ORDER BY
          cp.nav_patient_id DESC) = 1) AS inav
    ON
      RTRIM(COALESCE(CONCAT(TRIM(met.network_mnemonic_cs), met.patient_market_urn_text), CONCAT(met.medical_record_num, met.facility_mnemonic_cs))) = RTRIM(COALESCE(inav.patient_market_urn, CONCAT(inav.medical_record_num, inav.facility_mnemonic_cs)))
      AND RTRIM(met.network_mnemonic_cs) = RTRIM(inav.network_mnemonic_cs)
    WHERE
      COALESCE(met.coid, cpid.coid, inav.coid) IS NOT NULL
      )
  )
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
