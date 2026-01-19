-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/act/j_cdm_adhoc_ca_proc_list_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT ca_proc_list.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_proc_list) AS a