CREATE OR REPLACE VIEW {{ params.param_rmt_mirrored_base_views_dataset_name }}.lu_remit_payer
AS SELECT
		lu_remit_payer.remit_payer_id,
		lu_remit_payer.payer_name,
		lu_remit_payer.address_1,
		lu_remit_payer.address_2,
		lu_remit_payer.city,
		lu_remit_payer.state,
		lu_remit_payer.zip,
		lu_remit_payer.country_cd,
		lu_remit_payer.payer_id_num,
		lu_remit_payer.source_system_code,
		lu_remit_payer.dw_last_update_date_time,
		lu_remit_payer.customer_cd
  FROM
     {{ params.param_mirroring_project_id }}.{{ params.param_rmt_mirrored_core_dataset_name }}.lu_remit_payer
;
