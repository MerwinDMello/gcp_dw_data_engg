CREATE OR REPLACE VIEW edwclm_bobj_views.lu_billing_provider
AS SELECT
		lu_billing_provider.bill_provider_sid,
		lu_billing_provider.bill_provider_name,
		lu_billing_provider.bill_provider_addr1,
		lu_billing_provider.bill_provider_addr2,
		lu_billing_provider.bill_provider_city,
		lu_billing_provider.bill_provider_st,
		lu_billing_provider.bill_provider_zip_cd,
		lu_billing_provider.bill_provider_npi,
		lu_billing_provider.dw_last_update_date_time,
		lu_billing_provider.source_system_code
  FROM
    edwclm_base_views.lu_billing_provider
;
