CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_ed_complaint AS SELECT
    ref_ed_complaint.company_code,
    ref_ed_complaint.coid,
    ref_ed_complaint.ed_complaint_code,
    ref_ed_complaint.ed_complaint_desc,
    ref_ed_complaint.active_ind,
    ref_ed_complaint.nomenclature_code,
    ref_ed_complaint.network_mnemonic_cs,
    ref_ed_complaint.facility_mnemonic_cs,
    ref_ed_complaint.source_system_code,
    ref_ed_complaint.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_ed_complaint
;
