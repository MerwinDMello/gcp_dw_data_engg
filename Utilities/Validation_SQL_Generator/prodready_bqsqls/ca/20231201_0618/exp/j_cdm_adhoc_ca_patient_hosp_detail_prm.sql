-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_patient_hosp_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT DISTINCT a.*
   FROM
     (SELECT DISTINCT caph.patient_hosp_sk,
                      83 AS patient_hosp_detail_measure_id,
                      cahs.pc4hospdatavrsn AS patient_hosp_detail_measure_value_text,
                      CAST(NULL AS INT64) AS patient_hosp_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospitalization_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cahs.full_server_nm) = upper(cas.server_name)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_hosp AS caph
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cahs.hospitalizationid = caph.source_patient_hosp_id
      AND cas.server_sk = caph.server_sk
      WHERE cahs.pc4hospdatavrsn IS NOT NULL
      UNION DISTINCT SELECT DISTINCT caph.patient_hosp_sk,
                                     84 AS patient_hosp_detail_measure_id,
                                     cahs.impactdatavrsn AS patient_hosp_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS patient_hosp_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospitalization_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cahs.full_server_nm) = upper(cas.server_name)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_hosp AS caph
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cahs.hospitalizationid = caph.source_patient_hosp_id
      AND cas.server_sk = caph.server_sk
      WHERE cahs.impactdatavrsn IS NOT NULL ) AS a) AS b