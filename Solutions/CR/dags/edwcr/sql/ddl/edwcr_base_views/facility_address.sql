CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.facility_address AS SELECT
    facility_address.coid,
    facility_address.company_code,
    facility_address.address_type_code,
    facility_address.data_source_code,
    facility_address.street_address_1_text,
    facility_address.street_address_2_text,
    facility_address.city_name,
    facility_address.state_code,
    facility_address.postal_code,
    facility_address.country_code,
    facility_address.zip_plus_four_code,
    facility_address.eff_from_date,
    facility_address.eff_to_date
  FROM
    {{ params.param_auth_base_views_dataset_name }}.facility_address
;
