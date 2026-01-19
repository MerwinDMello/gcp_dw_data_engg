CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_facility AS SELECT
    fa.facility_sk,
    fa.company_code,
    fa.coid,
    fa.unit_num,
    fa.facility_name,
    fa.bed_size_cnt,
    fa.prof_service_area_sk,
    fa.active_ind,
    fa.external_ind,
    fa.valid_from_date_time,
    fa.market_sk,
    cl.facility_mnemonic,
    cl.network_mnemonic_cs,
    fa.source_system_txt,
    fa.source_system_code,
    fa.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_facility AS fa
    LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.clinical_facility AS cl ON rtrim(cl.coid) = rtrim(fa.coid)
;
