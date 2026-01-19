-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_diagnosis_list_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT DISTINCT a.*
   FROM
     (SELECT NULL AS diagnosis_list_sk,
             rcdc.diagnosis_category_id AS diagnosis_category_id,
             ccs.server_sk AS server_sk,
             stg.id AS source_diagnosis_list_id,
             stg.diagnosis AS diagnosis_name,
             stg.icd_9_code AS source_diagnosis_code_text,
             9 AS diagnosis_type_code,
             stg.wrkgrpcode AS work_group_code,
             stg.dbtype AS db_type_text,
             CASE stg.inactive
                 WHEN 1 THEN 'N'
                 WHEN 2 THEN 'Y'
             END AS active_ind,
             'C' AS source_system_code,
             timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ref_ca_diagnosis_category AS rcdc
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(stg.dxmcategory, '')) = upper(coalesce(rcdc.diagnosis_category_name, ''))
      AND upper(coalesce(stg.dxscategory, '')) = upper(coalesce(rcdc.diagnosis_sub_category_name, ''))
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS ccs
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ccs.server_name)
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_diagnosis_list AS ch
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ch.server_sk = ccs.server_sk
      AND ch.source_diagnosis_list_id = stg.id
      WHERE ch.server_sk IS NULL
        AND ch.source_diagnosis_list_id IS NULL ) AS a) AS b