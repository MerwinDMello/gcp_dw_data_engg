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
  FROM (
    SELECT
      format('%20d', count(*)) AS source_string
    FROM (
      SELECT
        DISTINCT COALESCE(t3.icd_oncology_code, '-99') AS cp_icd_oncology_code,
        COALESCE(t3.icd_oncology_site_desc, 'Unknown Description') AS cp_icd_oncology_site_desc,
        COALESCE(coalesce(t7.tumor_group,
            t3.icd_oncology_site_desc), 'Unknown Description') AS cp_icd_oncology_group_name,
        t1.master_lookup_sid AS cr_tumor_primary_site_id,
        t4.tumor_type_id AS cn_tumor_type_id,
        t5.tumor_type_id AS cn_general_tumor_type_id,
        t6.tumor_type_id AS cn_navque_tumor_type_id,
        t1.lookup_code AS cr_icd_oncology_code,
        t1.lookup_desc AS cr_icd_oncology_site_desc,
        t4.tumor_type_group_name AS cn_tumor_group_name,
        t4.tumor_type_desc AS cn_tumor_type_desc,
        t5.tumor_type_group_name AS cn_general_tumor_group_name,
        t5.tumor_type_desc AS cn_general_tumor_type_desc,
        t6.tumor_type_desc AS cn_navque_tumor_type_desc
      FROM (
        SELECT
          DISTINCT t4_0.*
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output AS t1_0
        INNER JOIN
          {{ params.param_cr_base_views_dataset_name }}.ref_icd_oncology AS t4_0
        ON
          UPPER(RTRIM(t4_0.icd_oncology_code)) = UPPER(RTRIM(t1_0.submitted_primary_icd_oncology_code))
          AND UPPER(RTRIM(t4_0.icd_oncology_code)) NOT IN('C421')) AS t3
      FULL OUTER JOIN
        {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t1
      ON
        UPPER(RTRIM(SUBSTR(t1.lookup_code, 1, 3))) = UPPER(RTRIM(SUBSTR(t3.icd_oncology_code, 1, 3)))
      LEFT OUTER JOIN (
        SELECT
          CASE
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('BREAST') THEN 'C509'
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('LUNG') THEN 'C349'
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('HEMATOLOGY') THEN 'C424'
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('GI ANAL') THEN 'C218'
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('GI BILE DUCT') THEN 'C249'
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('GI COLON') THEN 'C189'
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('GI ESOPHAGEAL') THEN 'C159'
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('GI GASTRIC') THEN 'C169'
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('GI LIVER') THEN 'C220'
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('GI PANCREATIC') THEN 'C259'
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('GI RECTAL') THEN 'C209'
            WHEN UPPER(RTRIM(t1_0.tumor_type_desc)) IN('GI MIXED TUMOR') THEN 'C269'
        END
          AS nav_icd_xwalk,
          t1_0.*
        FROM
          {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS t1_0
        WHERE
          UPPER(RTRIM(t1_0.tumor_type_group_name)) NOT IN('GENERAL',
            'NAVQ',
            'NULL',
            'OTHER')
          AND t1_0.tumor_type_group_name IS NOT NULL ) AS t4
      ON
        UPPER(RTRIM(SUBSTR(t1.lookup_code, 1, 3))) = UPPER(RTRIM(SUBSTR(t4.nav_icd_xwalk, 1, 3)))
      LEFT OUTER JOIN (
        SELECT
          CASE
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('ADRENAL') THEN 'C749'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('BONE (OSTEOSARCOMA)') THEN 'C419'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('CENTRAL NERVOUS SYSTEM (BRAIN, SPINAL CORD)') THEN 'C710'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('CERVIX') THEN 'C539'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('EWING SARCOMA') THEN 'C499'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('EYE') THEN 'C699'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('FALLOPIAN TUBE') THEN 'C579'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('KAPOSI SARCOMA') THEN 'C499'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('KIDNEY (RENAL)') THEN 'C649'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('LACRIMAL GLAND') THEN 'C695'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('LARYNX') THEN 'C329'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('LUNG') THEN 'C349'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('MELANOMA') THEN 'C449'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('MERKEL CELL') THEN 'C449'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('MESOTHELIOMA (NON-LUNG)') THEN 'C809'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('MYELODYSPLASIA') THEN 'C424'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('NASAL CAVITY') THEN 'C300'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('NEURO ENDOCRINE') THEN 'C809'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('ORAL') THEN 'C069'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('OTHER') THEN 'C809'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('OVARY') THEN 'C569'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('PAROTID GLAND') THEN 'C079'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('PENIS') THEN 'C609'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('PERITONEUM') THEN 'C482'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('PHARYNX') THEN 'C148'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('PLACENTA') THEN 'C589'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('PROSTATE') THEN 'C619'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('SALIVARY GLAND') THEN 'C089'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('SCROTUM') THEN 'C639'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('SKIN (NON MELANOMA)') THEN 'C449'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('SOFT TISSUE') THEN 'C499'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('SUBMANDIBULAR GLAND') THEN 'C779'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('THYMUS') THEN 'C379'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('TESTIS') THEN 'C629'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('THYROID') THEN 'C739'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('TUMOR OF UNKNOWN PRIMARY') THEN 'C809'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('URETER') THEN 'C669'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('URETHRA') THEN 'C689'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('UTERINE') THEN 'C559'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('VAGINA') THEN 'C529'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('VULVA') THEN 'C519'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('BLADDER') THEN 'C679'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('BREAST') THEN 'C509'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('GI ANAL') THEN 'C218'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('GI BILE DUCT') THEN 'C249'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('GI COLON') THEN 'C189'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('GI ESOPHAGEAL') THEN 'C159'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('GI GASTRIC') THEN 'C169'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('GI LIVER') THEN 'C220'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('GI PANCREATIC') THEN 'C259'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('GI RECTAL') THEN 'C209'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('GI MIXED TUMOR') THEN 'C269'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('HEMATOLOGY') THEN 'C424'
            ELSE '-99'
        END
          AS nav_general_icd_xwalk,
          t1_0.*
        FROM
          {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS t1_0
        WHERE
          UPPER(RTRIM(t1_0.tumor_type_group_name)) IN('GENERAL') ) AS t5
      ON
        UPPER(RTRIM(SUBSTR(t1.lookup_code, 1, 3))) = UPPER(RTRIM(SUBSTR(t5.nav_general_icd_xwalk, 1, 3)))
      LEFT OUTER JOIN (
        SELECT
          CASE
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('GYN') THEN 'C579'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('LUNG') THEN 'C349'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('BREAST') THEN 'C509'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('COLON') THEN 'C189'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('SARCOMA') THEN 'C499'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('COMPLEX GI') THEN 'C269'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('NEURO') THEN 'C719'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('BENIGN NEURO') THEN 'C809'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('ACUTE LEUKEMIA - MDS') THEN 'C424'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('LYMPHOMA') THEN 'C424'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('MET TO BRAIN/NEURO - HIGH RISK') THEN 'C719'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('MET TO BRAIN/NEURO') THEN 'C719'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('SARCOMA - OTHER') THEN 'C809'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('SARCOMA - SOFT TISSUE') THEN 'C499'
            WHEN UPPER(TRIM(t1_0.tumor_type_desc)) IN('SARCOMA - BONE') THEN 'C419'
        END
          AS navq_icd_xwalk,
          t1_0.*
        FROM
          {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS t1_0
        WHERE
          UPPER(RTRIM(t1_0.tumor_type_group_name)) IN('NAVQ') ) AS t6
      ON
        UPPER(RTRIM(SUBSTR(t1.lookup_code, 1, 3))) = UPPER(RTRIM(SUBSTR(t6.navq_icd_xwalk, 1, 3)))
      LEFT OUTER JOIN (
        SELECT
          CASE
            WHEN UPPER(RTRIM(t2.icd_oncology_code)) IN('C23', 'C21', 'C24', 'C15', 'C22', 'C25', 'C20', 'C19', 'C16', 'C26', 'C17') THEN 'Complex GI'
            WHEN UPPER(RTRIM(t2.icd_oncology_code)) IN('C51',
            'C52',
            'C53',
            'C54',
            'C55',
            'C56',
            'C57') THEN 'Gynecological'
        END
          AS tumor_group,
          t2.icd_oncology_code,
          t2.source_system_code
        FROM (
          SELECT
            DISTINCT SUBSTR(t1_0.icd_oncology_code, 1, 3) AS icd_oncology_code,
            t1_0.icd_oncology_site_desc,
            t1_0.source_system_code
          FROM
            {{ params.param_cr_base_views_dataset_name }}.ref_icd_oncology AS t1_0) AS t2) AS t7
      ON
        UPPER(RTRIM(SUBSTR(t1.lookup_code, 1, 3))) = UPPER(RTRIM(SUBSTR(t7.icd_oncology_code, 1, 3)))
      WHERE
        CAST(t1.lookup_id AS FLOAT64) IN(18.0) ) AS a
  )
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver)
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
