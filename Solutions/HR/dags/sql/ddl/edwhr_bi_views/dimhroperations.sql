-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_BI_Views/dimhroperations.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.dimhroperations AS SELECT
    ref_hr_operations.process_level_code,
    CASE
      WHEN upper(ref_hr_operations.business_unit_name) = 'HEALTHTRUST WORKFORCE SOLN' THEN 'HWS'
      WHEN upper(ref_hr_operations.business_unit_name) = 'HUMAN RESOURCES GROUP' THEN 'HRG'
      ELSE ref_hr_operations.business_unit_name
    END AS business_unit_name,
    ref_hr_operations.business_unit_segment_name
  FROM
    {{ params.param_hr_base_views_dataset_name }}.ref_hr_operations
;
