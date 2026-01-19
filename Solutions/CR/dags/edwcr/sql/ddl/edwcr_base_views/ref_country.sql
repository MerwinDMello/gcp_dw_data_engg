CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_country AS SELECT
    country.country_code,
    country.country_name,
    country.country_num,
    country.source_system_code
  FROM
    {{ params.param_pf_base_views_dataset_name }}.country
;
