-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cancer_patient_outcome_timeliness.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cancer_patient_outcome_timeliness AS SELECT
    a.cancer_patient_tumor_driver_sk,
    a.cancer_patient_driver_sk,
    a.cancer_tumor_driver_sk,
    a.coid,
    a.company_code,
    a.length_to_chemo_day_num,
    a.length_to_hormone_day_num,
    a.length_to_immuno_day_num,
    a.length_to_surgery_day_num,
    a.length_to_radiation_day_num,
    a.length_to_transplant_day_num,
    a.length_to_first_treatment_day_num,
    a.first_surgery_chemo_elapsed_day_num,
    a.first_chemo_surgery_elapsed_day_num,
    a.radiation_elapsed_day_num,
    a.length_to_surgery_last_contact_day_num,
    a.length_to_diagnosis_last_contact_day_num,
    a.length_to_diagnosis_last_contact_mth_num,
    a.last_contact_date,
    a.admission_date,
    a.rln10_concordant_ind,
    a.rln12_concordant_ind,
    a.act_concordant_ind,
    a.bcs_concordant_ind,
    a.bcsrt_concordant_ind,
    a.cbrrt_concordant_ind,
    a.cerct_concrodant_ind,
    a.cerrt_concrodant_ind,
    a.endctrt_concordant_ind,
    a.endlrc_concordant_ind,
    a.g15rlnc_concordant_ind,
    a.lct_concordant_ind,
    a.ht_concordant_ind,
    a.lnosurg_concordant_ind,
    a.mac_concordant_ind,
    a.mastrt_concordant_ind,
    a.nb_concordant_ind,
    a.ovsal_concordant_ind,
    a.recrtct_concordance_ind,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_outcome_timeliness AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
