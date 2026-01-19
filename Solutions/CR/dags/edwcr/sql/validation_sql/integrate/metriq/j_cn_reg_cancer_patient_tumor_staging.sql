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

  SET srctablename = concat(srcdataset_id, '.', 'cancer_patient_tumor_driver');

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
                             ORDER BY a.cancer_patient_tumor_driver_sk,
                                      a.cancer_patient_driver_sk,
                                      a.cancer_tumor_driver_sk,
                                      upper(a.cancer_stage_class_method_code),
                                      upper(cancer_stage_type_code)) AS cancer_patient_tumor_staging_sk,
                            a.cancer_patient_tumor_driver_sk,
                            a.cancer_patient_driver_sk,
                            a.cancer_tumor_driver_sk,
                            a.coid,
                            a.company_code,
                            a.best_cs_summary_desc,
                            a.best_cs_tnm_desc,
                            a.tumor_size_num_text,
                            a.cancer_stage_code,
                            a.cancer_stage_class_method_code,
                            cancer_stage_type_code AS cancer_stage_type_code,
                            a.cancer_stage_result_text,
                            a.ajcc_stage_desc,
                            a.tumor_size_summary_desc,
                            a.source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                      cptd.cancer_patient_driver_sk,
                      cptd.cancer_tumor_driver_sk,
                      cptd.coid,
                      cptd.company_code,
                      cr.best_cs_summary_desc,
                      cr.best_cs_tnm_desc,
                      cr.tumor_size_num_text,
                      cnps.cancer_stage_code,
                      coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                      coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                      coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                      cr.ajcc_stage_desc,
                      cr.tumor_size_summary_desc,
                      cptd.source_system_code,
                      cptd.dw_last_update_date_time
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage AS ras
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'T' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'T'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage AS ras
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'N' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'N'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage AS ras
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'M' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'M'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage AS ras
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
           AND cps.cancer_stage_type_code IS NULL ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
      AND cnps.cancer_staging_type_code IS NULL
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage AS ras
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'T' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'T'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage AS ras
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'N' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'N'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage AS ras
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'M' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'M'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage AS ras
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
           AND cps.cancer_stage_type_code IS NULL ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
      AND cnps.cancer_staging_type_code IS NULL
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL ) AS a
   CROSS JOIN UNNEST(ARRAY[ a.cancer_staging_type_code ]) AS cancer_stage_type_code) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_staging)
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
