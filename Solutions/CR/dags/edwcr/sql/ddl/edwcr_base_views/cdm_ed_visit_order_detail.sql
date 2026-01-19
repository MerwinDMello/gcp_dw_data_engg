CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_ed_visit_order_detail
   OPTIONS(description='Emergency department data at the order level for a patient. Includes treatments, procedures, and meditcations ordered for a patient.')
  AS SELECT
      cdm_ed_visit_order_detail.patient_dw_id,
      cdm_ed_visit_order_detail.order_urn,
      cdm_ed_visit_order_detail.coid,
      cdm_ed_visit_order_detail.company_code,
      cdm_ed_visit_order_detail.pat_acct_num,
      cdm_ed_visit_order_detail.order_date_time,
      cdm_ed_visit_order_detail.order_proc_num,
      cdm_ed_visit_order_detail.order_proc_mnemonic_cs,
      cdm_ed_visit_order_detail.order_proc_name,
      cdm_ed_visit_order_detail.source_system_code,
      cdm_ed_visit_order_detail.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_ed_visit_order_detail
  ;
