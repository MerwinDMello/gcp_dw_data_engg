-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/act/j_cr_ro_rad_onc_activity_transaction.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.rad_onc_activity_transaction
WHERE rad_onc_activity_transaction.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE