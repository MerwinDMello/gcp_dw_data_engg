-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_mt_ref_ajcc_stage.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_lookup_code_stg
WHERE ref_lookup_code_stg.lookup_id = 4040
  AND ref_lookup_code_stg.lookup_desc IS NOT NULL
  AND upper(trim(ref_lookup_code_stg.lookup_desc)) <> ''