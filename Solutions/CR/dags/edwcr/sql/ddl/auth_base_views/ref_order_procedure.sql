CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.ref_order_procedure
AS SELECT * FROM {{ params.param_clinical_dataset_name }}.ref_order_procedure;