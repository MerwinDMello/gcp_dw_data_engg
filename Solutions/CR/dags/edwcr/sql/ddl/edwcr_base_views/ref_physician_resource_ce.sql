CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_physician_resource_ce AS SELECT
    ref_physician_resource_ce.hcp_dw_id,
    ref_physician_resource_ce.company_code,
    ref_physician_resource_ce.coid,
    ref_physician_resource_ce.nppes_taxonomy_code,
    ref_physician_resource_ce.national_provider_id,
    ref_physician_resource_ce.physician_name,
    ref_physician_resource_ce.physician_group_name,
    ref_physician_resource_ce.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_physician_resource_ce
;
