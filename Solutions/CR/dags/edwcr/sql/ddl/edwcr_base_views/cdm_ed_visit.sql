CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_ed_visit
   OPTIONS(description='Emergency department data at the patient level.')
  AS SELECT
      cdm_ed_visit.patient_dw_id,
      cdm_ed_visit.coid,
      cdm_ed_visit.company_code,
      cdm_ed_visit.pat_acct_num,
      cdm_ed_visit.reason_for_visit_text,
      cdm_ed_visit.chief_complaint_query_mnemonic_cs,
      cdm_ed_visit.ed_complaint_desc,
      cdm_ed_visit.arrival_date_time,
      cdm_ed_visit.depart_date_time,
      cdm_ed_visit.admit_date_time,
      cdm_ed_visit.admit_ind,
      cdm_ed_visit.source_system_code,
      cdm_ed_visit.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_ed_visit
  ;
