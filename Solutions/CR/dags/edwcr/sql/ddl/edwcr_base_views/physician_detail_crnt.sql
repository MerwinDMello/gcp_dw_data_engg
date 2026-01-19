CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.physician_detail_crnt AS SELECT
    physician_detail_crnt.company_code,
    physician_detail_crnt.coid,
    physician_detail_crnt.facility_physician_num,
    physician_detail_crnt.unit_num,
    physician_detail_crnt.physician_npi,
    physician_detail_crnt.dim_physician_name_child,
    physician_detail_crnt.upin_num,
    physician_detail_crnt.physician_name,
    physician_detail_crnt.physician_last_name,
    physician_detail_crnt.physician_first_name,
    physician_detail_crnt.physician_middle_name,
    physician_detail_crnt.physician_name_prefix,
    physician_detail_crnt.physician_name_suffix,
    physician_detail_crnt.physician_status_code,
    physician_detail_crnt.patfin_physician_dw_id,
    physician_detail_crnt.social_security_num,
    physician_detail_crnt.physician_birth_date,
    physician_detail_crnt.medical_specialty_code,
    physician_detail_crnt.dim_physician_spcl_name_child,
    physician_detail_crnt.physician_group_npi,
    physician_detail_crnt.physician_group_num,
    physician_detail_crnt.physician_group_name,
    physician_detail_crnt.source_system_code,
    physician_detail_crnt.dw_last_update_date_time
  FROM
    {{ params.param_pf_base_views_dataset_name }}.physician_detail_crnt
;
