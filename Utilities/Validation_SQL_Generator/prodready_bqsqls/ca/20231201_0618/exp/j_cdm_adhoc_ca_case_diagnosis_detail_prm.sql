-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_case_diagnosis_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT DISTINCT a.*
   FROM
     (SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                      cardioaccess_diagnosis_stg.full_server_nm,
                      210 AS diagnosis_detail_measure_id,
                      cardioaccess_diagnosis_stg.diagcateg AS diagnosis_detail_measure_value_text,
                      CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.diagcateg IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     211 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code1 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code1 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     212 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code2 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code2 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     213 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code3 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code3 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     214 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code4 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code4 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     215 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code5 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code5 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     216 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code6 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code6 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     217 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code7 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code7 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     218 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code8 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code8 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     219 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code9 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code9 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     220 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code10 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code10 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     221 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code11 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code11 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     222 AS diagnosis_detail_measure_id,
                                     cardioaccess_diagnosis_stg.code12 AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.code12 IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     223 AS diagnosis_detail_measure_id,
                                     diagnosis_detail_measure_value_text AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      CROSS JOIN UNNEST(ARRAY[ substr(cardioaccess_diagnosis_stg.olddiagname, 1, 55) ]) AS diagnosis_detail_measure_value_text
      WHERE diagnosis_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     224 AS diagnosis_detail_measure_id,
                                     diagnosis_detail_measure_value_text AS diagnosis_detail_measure_value_text,
                                     CAST(NULL AS INT64) AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      CROSS JOIN UNNEST(ARRAY[ substr(cardioaccess_diagnosis_stg.class, 1, 55) ]) AS diagnosis_detail_measure_value_text
      WHERE diagnosis_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT DISTINCT cardioaccess_diagnosis_stg.diagnosisid,
                                     cardioaccess_diagnosis_stg.full_server_nm,
                                     225 AS diagnosis_detail_measure_id,
                                     CAST(NULL AS STRING) AS diagnosis_detail_measure_value_text,
                                     cardioaccess_diagnosis_stg.recur AS diagnosis_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosis_stg
      WHERE cardioaccess_diagnosis_stg.recur IS NOT NULL ) AS a) AS b