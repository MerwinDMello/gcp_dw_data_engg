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

  SET srctablename = concat(srcdataset_id, '.', 'cancer_patient_driver');

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
                             ORDER BY a.cancer_patient_driver_sk,
                                      a.t_sk,
                                      a.cr_patient_id,
                                      a.cn_patient_id,
                                      a.cp_patient_id,
                                      a.cr_tumor_primary_site_id,
                                      a.cn_tumor_type_id,
                                      upper(a.cp_icd_oncology_code)) AS cancer_patient_tumor_driver_sk,
                            a.*,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT cpd.coid,
                      cpd.company_code,
                      cpd.cancer_patient_driver_sk,
                      t.cancer_tumor_driver_sk AS t_sk,
                      cpt.cr_patient_id,
                      cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                      CAST(NULL AS NUMERIC) AS cn_patient_id,
                      CAST(NULL AS INT64) AS cn_tumor_type_id,
                      CAST(NULL AS NUMERIC) AS cp_patient_id,
                      CAST(NULL AS STRING) AS cp_icd_oncology_code,
                      cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
      INNER JOIN
        (SELECT cancer_tumor_driver.*
         FROM {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver 
         QUALIFY row_number() OVER (PARTITION BY cancer_tumor_driver.cr_tumor_primary_site_id
                                                                                                ORDER BY cancer_tumor_driver.cancer_tumor_driver_sk) = 1) AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
      WHERE cpd.cn_patient_id IS NULL
        AND cpd.cp_patient_id IS NULL
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t2.cancer_tumor_driver_sk AS t_sk,
                                     CAST(NULL AS INT64) AS cr_patient_id,
                                     CAST(NULL AS INT64) AS cr_tumor_primary_site_id,
                                     CAST(NULL AS NUMERIC) AS cn_patient_id,
                                     CAST(NULL AS INT64) AS cn_tumor_type_id,
                                     cpnt.patient_dw_id AS cp_patient_id,
                                     cpnt.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN
        (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                         cancer_patient_id_output.submitted_primary_icd_oncology_code
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt ON cpd.cp_patient_id = cpnt.patient_dw_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
      ON upper(rtrim(cpnt.submitted_primary_icd_oncology_code)) = upper(rtrim(t2.cp_icd_oncology_code))
      WHERE cpd.cr_patient_id IS NULL
        AND cpd.cn_patient_id IS NULL QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                              upper(cpnt.submitted_primary_icd_oncology_code)
                                                                 ORDER BY t_sk) = 1
      UNION DISTINCT SELECT cpd.coid,
                            cpd.company_code,
                            cpd.cancer_patient_driver_sk,
                            t1.cancer_tumor_driver_sk AS t_sk,
                            CAST(NULL AS INT64) AS cr_patient_id,
                            CAST(NULL AS INT64) AS cr_tumor_primary_site_id,
                            cpnt.nav_patient_id AS cn_patient_id,
                            cpnt.tumor_type_id AS cn_tumor_type_id,
                            CAST(NULL AS NUMERIC) AS cp_patient_id,
                            CAST(NULL AS STRING) AS cp_icd_oncology_code,
                            cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
      ON CASE upper(rtrim(rtt.tumor_type_group_name))
          WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
          WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
          ELSE t1.cn_tumor_type_id
      END = cpnt.tumor_type_id
      WHERE cpd.cr_patient_id IS NULL
        AND cpd.cp_patient_id IS NULL QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                              cn_tumor_type_id
                                                                 ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t.cancer_tumor_driver_sk AS t_sk,
                                     cpt.cr_patient_id,
                                     cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                                     CAST(NULL AS NUMERIC) AS cn_patient_id,
                                     CAST(NULL AS INT64) AS cn_tumor_type_id,
                                     cpnt.patient_dw_id AS cp_patient_id,
                                     cpnt.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
      ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
      INNER JOIN
        (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                         cancer_patient_id_output.submitted_primary_icd_oncology_code
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt ON cpd.cp_patient_id = cpnt.patient_dw_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
      ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt.submitted_primary_icd_oncology_code))
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NULL
        AND t.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                         cr_tumor_primary_site_id
                                                                                            ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t2.cancer_tumor_driver_sk AS t_sk,
                                     CAST(NULL AS INT64) AS cr_patient_id,
                                     CAST(NULL AS INT64) AS cr_tumor_primary_site_id,
                                     CAST(NULL AS NUMERIC) AS cn_patient_id,
                                     CAST(NULL AS INT64) AS cn_tumor_type_id,
                                     cpnt.patient_dw_id AS cp_patient_id,
                                     cpnt.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN
        (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                         cancer_patient_id_output.submitted_primary_icd_oncology_code
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt ON cpd.cp_patient_id = cpnt.patient_dw_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
      ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt.submitted_primary_icd_oncology_code))
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NULL
        AND (cpnt.patient_dw_id,
             upper(cpnt.submitted_primary_icd_oncology_code)) NOT IN
          (SELECT DISTINCT AS STRUCT cpnt_0.patient_dw_id,
                                     upper(cpnt_0.submitted_primary_icd_oncology_code) AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
           ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt_0 ON cpd_0.cp_patient_id = cpnt_0.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2_0
           ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt_0.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NULL
             AND t.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                  upper(cpnt.submitted_primary_icd_oncology_code)
                                                                                                     ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t.cancer_tumor_driver_sk AS t_sk,
                                     cpt.cr_patient_id,
                                     cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                                     CAST(NULL AS NUMERIC) AS cn_patient_id,
                                     CAST(NULL AS INT64) AS cn_tumor_type_id,
                                     CAST(NULL AS NUMERIC) AS cp_patient_id,
                                     CAST(NULL AS STRING) AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
      ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NULL
        AND (cpt.cr_patient_id,
             cpt.tumor_primary_site_id) NOT IN
          (SELECT DISTINCT AS STRUCT cpt_0.cr_patient_id,
                                     cpt_0.tumor_primary_site_id AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t_0
           ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt ON cpd_0.cp_patient_id = cpnt.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
           ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NULL
             AND t_0.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                  cr_tumor_primary_site_id
                                                                                                     ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t.cancer_tumor_driver_sk AS t_sk,
                                     cpt.cr_patient_id,
                                     cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                                     cpnt.nav_patient_id AS cn_patient_id,
                                     cpnt.tumor_type_id AS cn_tumor_type_id,
                                     CAST(NULL AS NUMERIC) AS cp_patient_id,
                                     CAST(NULL AS STRING) AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
      ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
      ON CASE upper(rtrim(rtt.tumor_type_group_name))
          WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
          WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
          ELSE t1.cn_tumor_type_id
      END = cpnt.tumor_type_id
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND t.cancer_tumor_driver_sk = t1.cancer_tumor_driver_sk QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                         cr_tumor_primary_site_id,
                                                                                                         cn_tumor_type_id
                                                                                            ORDER BY t_sk) = 1
      UNION DISTINCT SELECT cpd.coid,
                            cpd.company_code,
                            cpd.cancer_patient_driver_sk,
                            t1.cancer_tumor_driver_sk AS t_sk,
                            CAST(NULL AS INT64) AS cr_patient_id,
                            CAST(NULL AS INT64) AS cr_tumor_primary_site_id,
                            cpnt.nav_patient_id AS cn_patient_id,
                            cpnt.tumor_type_id AS cn_tumor_type_id,
                            CAST(NULL AS NUMERIC) AS cp_patient_id,
                            CAST(NULL AS STRING) AS cp_icd_oncology_code,
                            cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
      ON CASE upper(rtrim(rtt.tumor_type_group_name))
          WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
          WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
          ELSE t1.cn_tumor_type_id
      END = cpnt.tumor_type_id
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND (cpnt.nav_patient_id,
             cpnt.tumor_type_id) NOT IN
          (SELECT DISTINCT AS STRUCT cpnt_0.nav_patient_id,
                                     cpnt_0.tumor_type_id AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
           ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1_0
           ON CASE upper(rtrim(rtt_0.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
              ELSE t1_0.cn_tumor_type_id
          END = cpnt_0.tumor_type_id
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t.cancer_tumor_driver_sk = t1_0.cancer_tumor_driver_sk ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                  cn_tumor_type_id
                                                                                                     ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t.cancer_tumor_driver_sk AS t_sk,
                                     cpt.cr_patient_id,
                                     cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                                     CAST(NULL AS NUMERIC) AS cn_patient_id,
                                     CAST(NULL AS INT64) AS cn_tumor_type_id,
                                     CAST(NULL AS NUMERIC) AS cp_patient_id,
                                     CAST(NULL AS STRING) AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
      ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND (cpt.cr_patient_id,
             cpt.tumor_primary_site_id) NOT IN
          (SELECT DISTINCT AS STRUCT cpt_0.cr_patient_id,
                                     cpt_0.tumor_primary_site_id
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t_0
           ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd_0.cn_patient_id = cpnt.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
           ON CASE upper(rtrim(rtt.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
              ELSE t1.cn_tumor_type_id
          END = cpnt.tumor_type_id
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t_0.cancer_tumor_driver_sk = t1.cancer_tumor_driver_sk ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                  cr_tumor_primary_site_id
                                                                                                     ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t1.cancer_tumor_driver_sk AS t_sk,
                                     CAST(NULL AS INT64) AS cr_patient_id,
                                     CAST(NULL AS INT64) AS cr_tumor_primary_site_id,
                                     cpnt.nav_patient_id AS cn_patient_id,
                                     cpnt.tumor_type_id AS cn_tumor_type_id,
                                     cpnt2.patient_dw_id AS cp_patient_id,
                                     cpnt2.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
      ON CASE upper(rtrim(rtt.tumor_type_group_name))
          WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
          WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
          ELSE t1.cn_tumor_type_id
      END = cpnt.tumor_type_id
      INNER JOIN
        (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                         cancer_patient_id_output.submitted_primary_icd_oncology_code
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
      ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
      WHERE cpd.cr_patient_id IS NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND t1.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                          cn_tumor_type_id,
                                                                                                          upper(cpnt2.submitted_primary_icd_oncology_code)
                                                                                             ORDER BY t_sk) = 1
      UNION DISTINCT SELECT cpd.coid,
                            cpd.company_code,
                            cpd.cancer_patient_driver_sk,
                            t1.cancer_tumor_driver_sk AS t_sk,
                            CAST(NULL AS INT64) AS cr_patient_id,
                            CAST(NULL AS INT64) AS cr_tumor_primary_site_id,
                            cpnt.nav_patient_id AS cn_patient_id,
                            cpnt.tumor_type_id AS cn_tumor_type_id,
                            CAST(NULL AS NUMERIC) AS cp_patient_id,
                            CAST(NULL AS STRING) AS cp_icd_oncology_code,
                            cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
      ON CASE upper(rtrim(rtt.tumor_type_group_name))
        WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
        WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
        ELSE t1.cn_tumor_type_id
    END = cpnt.tumor_type_id
      WHERE cpd.cr_patient_id IS NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND (cpnt.nav_patient_id,
             cpnt.tumor_type_id) NOT IN
          (SELECT DISTINCT AS STRUCT cpnt_0.nav_patient_id,
                                     cpnt_0.tumor_type_id
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1_0
           ON CASE upper(rtrim(rtt_0.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
              ELSE t1_0.cn_tumor_type_id
          END = cpnt_0.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
           ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1_0.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                   cn_tumor_type_id
                                                                                                      ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t2.cancer_tumor_driver_sk AS t_sk,
                                     CAST(NULL AS INT64) AS cr_patient_id,
                                     CAST(NULL AS INT64) AS cr_tumor_primary_site_id,
                                     CAST(NULL AS NUMERIC) AS cn_patient_id,
                                     CAST(NULL AS INT64) AS cn_tumor_type_id,
                                     cpnt2.patient_dw_id AS cp_patient_id,
                                     cpnt2.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN
        (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                         cancer_patient_id_output.submitted_primary_icd_oncology_code
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
      ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
      WHERE cpd.cr_patient_id IS NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND (cpnt2.patient_dw_id,
             upper(cpnt2.submitted_primary_icd_oncology_code)) NOT IN
          (SELECT DISTINCT AS STRUCT cpnt2_0.patient_dw_id,
                                     upper(cpnt2_0.submitted_primary_icd_oncology_code) AS submitted_primary_icd_oncology_code
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd_0.cn_patient_id = cpnt.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
           ON CASE upper(rtrim(rtt.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
              ELSE t1.cn_tumor_type_id
          END = cpnt.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2_0 ON cpd_0.cp_patient_id = cpnt2_0.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2_0
           ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2_0.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                   upper(cpnt2.submitted_primary_icd_oncology_code)
                                                                                                      ORDER BY t_sk) = 1
      UNION DISTINCT SELECT cpd.coid,
                            cpd.company_code,
                            cpd.cancer_patient_driver_sk,
                            t1.cancer_tumor_driver_sk AS t_sk,
                            cpt.cr_patient_id,
                            cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                            cpnt.nav_patient_id AS cn_patient_id,
                            cpnt.tumor_type_id AS cn_tumor_type_id,
                            cpnt2.patient_dw_id AS cp_patient_id,
                            cpnt2.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                            cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
      ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
      ON CASE upper(rtrim(rtt.tumor_type_group_name))
          WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
          WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
          ELSE t1.cn_tumor_type_id
      END = cpnt.tumor_type_id
      INNER JOIN
        (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                         cancer_patient_id_output.submitted_primary_icd_oncology_code
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
      ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND t1.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
        AND t.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk QUALIFY row_number() OVER (PARTITION BY upper(cpnt2.submitted_primary_icd_oncology_code),
                                                                                                         cpd.cancer_patient_driver_sk,
                                                                                                         cr_tumor_primary_site_id,
                                                                                                         cn_tumor_type_id
                                                                                            ORDER BY t_sk) = 1
      UNION DISTINCT SELECT cpd.coid,
                            cpd.company_code,
                            cpd.cancer_patient_driver_sk,
                            t1.cancer_tumor_driver_sk AS t_sk,
                            cpt.cr_patient_id,
                            cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                            cpnt.nav_patient_id AS cn_patient_id,
                            cpnt.tumor_type_id AS cn_tumor_type_id,
                            CAST(NULL AS NUMERIC) AS cp_patient_id,
                            CAST(NULL AS STRING) AS cp_icd_oncology_code,
                            cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
      ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
      ON CASE upper(rtrim(rtt.tumor_type_group_name))
          WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
          WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
          ELSE t1.cn_tumor_type_id
      END = cpnt.tumor_type_id
      INNER JOIN
        (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                         cancer_patient_id_output.submitted_primary_icd_oncology_code
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
      ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND t1.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
        AND (t.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk
             OR t1.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
        AND (cpt.cr_patient_id,
             cpt.tumor_primary_site_id) NOT IN
          (SELECT AS STRUCT cpt_0.cr_patient_id,
                            cpt_0.tumor_primary_site_id
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t_0
           ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1_0
           ON CASE upper(rtrim(rtt_0.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
              ELSE t1_0.cn_tumor_type_id
          END = cpnt_0.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2_0 ON cpd_0.cp_patient_id = cpnt2_0.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2_0
           ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2_0.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1_0.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
             AND t_0.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                    cn_tumor_type_id,
                                                                                                                    cr_tumor_primary_site_id
                                                                                                       ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t1.cancer_tumor_driver_sk AS t_sk,
                                     CAST(NULL AS INT64) AS cr_patient_id,
                                     CAST(NULL AS INT64) AS cr_tumor_primary_site_id,
                                     cpnt.nav_patient_id AS cn_patient_id,
                                     cpnt.tumor_type_id AS cn_tumor_type_id,
                                     cpnt2.patient_dw_id AS cp_patient_id,
                                     cpnt2.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
      ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
      ON CASE upper(rtrim(rtt.tumor_type_group_name))
          WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
          WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
          ELSE t1.cn_tumor_type_id
      END = cpnt.tumor_type_id
      INNER JOIN
        (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                         cancer_patient_id_output.submitted_primary_icd_oncology_code
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
      ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND t1.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
        AND (t.cancer_tumor_driver_sk <> t1.cancer_tumor_driver_sk
             OR t.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
        AND (cpnt2.patient_dw_id,
             upper(cpnt2.submitted_primary_icd_oncology_code)) NOT IN
          (SELECT AS STRUCT cpnt2_0.patient_dw_id,
                            upper(cpnt2_0.submitted_primary_icd_oncology_code) AS submitted_primary_icd_oncology_code
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t_0
           ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1_0
           ON CASE upper(rtrim(rtt_0.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
              ELSE t1_0.cn_tumor_type_id
          END = cpnt_0.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2_0 ON cpd_0.cp_patient_id = cpnt2_0.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2_0
           ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2_0.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1_0.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
             AND t_0.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                    cn_tumor_type_id
                                                                                                       ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t.cancer_tumor_driver_sk AS t_sk,
                                     cpt.cr_patient_id,
                                     cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                                     CAST(NULL AS NUMERIC) AS cn_patient_id,
                                     CAST(NULL AS INT64) AS cn_tumor_type_id,
                                     cpnt2.patient_dw_id AS cp_patient_id,
                                     cpnt2.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
      ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
      ON CASE upper(rtrim(rtt.tumor_type_group_name))
          WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
          WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
          ELSE t1.cn_tumor_type_id
      END = cpnt.tumor_type_id
      INNER JOIN
        (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                         cancer_patient_id_output.submitted_primary_icd_oncology_code
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
      ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND t2.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
        AND (t.cancer_tumor_driver_sk <> t1.cancer_tumor_driver_sk
             OR t1.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
        AND (cpt.cr_patient_id,
             cpt.tumor_primary_site_id) NOT IN
          (SELECT AS STRUCT cpt_0.cr_patient_id,
                            cpt_0.tumor_primary_site_id
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t_0
           ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1_0
           ON CASE upper(rtrim(rtt_0.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
              ELSE t1_0.cn_tumor_type_id
          END = cpnt_0.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2_0 ON cpd_0.cp_patient_id = cpnt2_0.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2_0
           ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2_0.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1_0.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
             AND t_0.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                    cr_tumor_primary_site_id
                                                                                                       ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t.cancer_tumor_driver_sk AS t_sk,
                                     cpt.cr_patient_id,
                                     cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                                     CAST(NULL AS NUMERIC) AS cn_patient_id,
                                     CAST(NULL AS INT64) AS cn_tumor_type_id,
                                     CAST(NULL AS NUMERIC) AS cp_patient_id,
                                     CAST(NULL AS STRING) AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
      ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND (cpt.cr_patient_id,
             cpt.tumor_primary_site_id) NOT IN
          (SELECT DISTINCT AS STRUCT cpt_0.cr_patient_id,
                                     cpt_0.tumor_primary_site_id AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t_0
           ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd_0.cn_patient_id = cpnt.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
           ON CASE upper(rtrim(rtt.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
              ELSE t1.cn_tumor_type_id
          END = cpnt.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
           ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
             AND (t_0.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk
                  OR t1.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
           UNION ALL SELECT DISTINCT AS STRUCT cpt_0.cr_patient_id,
                                               cpt_0.tumor_primary_site_id AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t_0
           ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd_0.cn_patient_id = cpnt.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
           ON CASE upper(rtrim(rtt.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
              ELSE t1.cn_tumor_type_id
          END = cpnt.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
           ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
             AND t_0.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
           UNION ALL SELECT DISTINCT AS STRUCT cpt_0.cr_patient_id,
                                               cpt_0.tumor_primary_site_id AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t_0
           ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd_0.cn_patient_id = cpnt.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
           ON CASE upper(rtrim(rtt.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
              ELSE t1.cn_tumor_type_id
          END = cpnt.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
           ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t2.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
             AND (t_0.cancer_tumor_driver_sk <> t1.cancer_tumor_driver_sk
                  OR t1.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk) ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                       cr_tumor_primary_site_id
                                                                                                          ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t1.cancer_tumor_driver_sk AS t_sk,
                                     CAST(NULL AS INT64) AS cr_patient_id,
                                     CAST(NULL AS INT64) AS cr_tumor_primary_site_id,
                                     cpnt.nav_patient_id AS cn_patient_id,
                                     cpnt.tumor_type_id AS cn_tumor_type_id,
                                     CAST(NULL AS NUMERIC) AS cp_patient_id,
                                     CAST(NULL AS STRING) AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
      ON CASE upper(rtrim(rtt.tumor_type_group_name))
          WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
          WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
          ELSE t1.cn_tumor_type_id
      END = cpnt.tumor_type_id
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND (cpnt.nav_patient_id,
             cpnt.tumor_type_id) NOT IN
          (SELECT DISTINCT AS STRUCT cpnt_0.nav_patient_id,
                                     cpnt_0.tumor_type_id AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
           ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1_0
           ON CASE upper(rtrim(rtt_0.tumor_type_group_name))
                WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
                WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
                ELSE t1_0.cn_tumor_type_id
            END = cpnt_0.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
           ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1_0.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
             AND (t.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk
                  OR t1_0.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
           UNION ALL SELECT DISTINCT AS STRUCT cpnt_0.nav_patient_id,
                                               cpnt_0.tumor_type_id AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
           ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1_0
           ON CASE upper(rtrim(rtt_0.tumor_type_group_name))
                WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
                WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
                ELSE t1_0.cn_tumor_type_id
            END = cpnt_0.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
           ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1_0.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
             AND t.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
           UNION ALL SELECT DISTINCT AS STRUCT cpnt_0.nav_patient_id,
                                               cpnt_0.tumor_type_id AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
           ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1_0
           ON CASE upper(rtrim(rtt_0.tumor_type_group_name))
                WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
                WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
                ELSE t1_0.cn_tumor_type_id
            END = cpnt_0.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
           ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1_0.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
             AND (t.cancer_tumor_driver_sk <> t1_0.cancer_tumor_driver_sk
                  OR t.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk) ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                      cn_tumor_type_id
                                                                                                         ORDER BY t_sk) = 1
      UNION DISTINCT SELECT DISTINCT cpd.coid,
                                     cpd.company_code,
                                     cpd.cancer_patient_driver_sk,
                                     t2.cancer_tumor_driver_sk AS t_sk,
                                     CAST(NULL AS INT64) AS cr_patient_id,
                                     CAST(NULL AS INT64) AS cr_tumor_primary_site_id,
                                     CAST(NULL AS NUMERIC) AS cn_patient_id,
                                     CAST(NULL AS INT64) AS cn_tumor_type_id,
                                     cpnt.patient_dw_id AS cp_patient_id,
                                     cpnt.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                                     cpd.source_system_code
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd
      INNER JOIN
        (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                         cancer_patient_id_output.submitted_primary_icd_oncology_code
         FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt ON cpd.cp_patient_id = cpnt.patient_dw_id
      INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2
      ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt.submitted_primary_icd_oncology_code))
      WHERE cpd.cr_patient_id IS NOT NULL
        AND cpd.cp_patient_id IS NOT NULL
        AND cpd.cn_patient_id IS NOT NULL
        AND (cpnt.patient_dw_id,
             upper(cpnt.submitted_primary_icd_oncology_code)) NOT IN
          (SELECT DISTINCT AS STRUCT cpnt2.patient_dw_id,
                                     upper(cpnt2.submitted_primary_icd_oncology_code) AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
           ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt_0.tumor_type_id = rtt.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
           ON CASE upper(rtrim(rtt.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
              ELSE t1.cn_tumor_type_id
          END = cpnt_0.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2_0
           ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t2_0.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
             AND (t.cancer_tumor_driver_sk <> t1.cancer_tumor_driver_sk
                  OR t1.cancer_tumor_driver_sk <> t2_0.cancer_tumor_driver_sk)
           UNION ALL SELECT DISTINCT AS STRUCT cpnt2.patient_dw_id,
                                               upper(cpnt2.submitted_primary_icd_oncology_code) AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
           ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt_0.tumor_type_id = rtt.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
           ON CASE upper(rtrim(rtt.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
              ELSE t1.cn_tumor_type_id
          END = cpnt_0.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2_0
           ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
             AND t.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk
           UNION ALL SELECT DISTINCT AS STRUCT cpnt2.patient_dw_id,
                                               upper(cpnt2.submitted_primary_icd_oncology_code) AS t1_sk
           FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd_0
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t
           ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
           INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpnt_0.tumor_type_id = rtt.tumor_type_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t1
           ON CASE upper(rtrim(rtt.tumor_type_group_name))
              WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
              WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
              ELSE t1.cn_tumor_type_id
          END = cpnt_0.tumor_type_id
           INNER JOIN
             (SELECT DISTINCT cancer_patient_id_output.patient_dw_id,
                              cancer_patient_id_output.submitted_primary_icd_oncology_code
              FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
           INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver AS t2_0
           ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
           WHERE cpd_0.cr_patient_id IS NOT NULL
             AND cpd_0.cp_patient_id IS NOT NULL
             AND cpd_0.cn_patient_id IS NOT NULL
             AND t1.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk
             AND (t.cancer_tumor_driver_sk <> t1.cancer_tumor_driver_sk
                  OR t.cancer_tumor_driver_sk <> t2_0.cancer_tumor_driver_sk) ) QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk,
                                                                                                                        upper(cpnt.submitted_primary_icd_oncology_code)
                                                                                                           ORDER BY t_sk) = 1 ) AS a) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_driver)
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
