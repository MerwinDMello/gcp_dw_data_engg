CREATE OR REPLACE VIEW {{ params.param_rmt_mirrored_base_views_dataset_name }}.lu_remit_provider
AS SELECT
		lu_remit_provider.remit_provider_id,
		lu_remit_provider.provider_name,
		lu_remit_provider.address_1,
		lu_remit_provider.address_2,
		lu_remit_provider.city,
		lu_remit_provider.state,
		lu_remit_provider.zip,
		lu_remit_provider.country_cd,
		lu_remit_provider.npi,
		lu_remit_provider.tax_id,
		lu_remit_provider.dw_last_update_date_time,
		lu_remit_provider.source_system_code,
		lu_remit_provider.customer_cd
  FROM
     {{ params.param_mirroring_project_id }}.{{ params.param_rmt_mirrored_core_dataset_name }}.lu_remit_provider
;
