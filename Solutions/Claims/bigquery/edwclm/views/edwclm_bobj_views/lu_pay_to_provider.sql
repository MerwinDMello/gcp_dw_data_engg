CREATE OR REPLACE VIEW edwclm_bobj_views.lu_pay_to_provider
AS SELECT
		lu_pay_to_provider.pay_to_provider_sid,
		lu_pay_to_provider.pay_to_provider_name,
		lu_pay_to_provider.pay_to_provider_addr1,
		lu_pay_to_provider.pay_to_provider_addr2,
		lu_pay_to_provider.pay_to_provider_city,
		lu_pay_to_provider.pay_to_provider_st,
		lu_pay_to_provider.pay_to_provider_zip_cd,
		lu_pay_to_provider.dw_last_update_date_time,
		lu_pay_to_provider.source_system_code
  FROM
    edwclm_base_views.lu_pay_to_provider
;
