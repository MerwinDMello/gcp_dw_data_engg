-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_Base_Views/pat_contact_consent.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.pat_contact_consent AS SELECT
    pat_contact_consent.patient_dw_id,
    pat_contact_consent.medical_record_num,
    pat_contact_consent.company_code,
    pat_contact_consent.coid,
    pat_contact_consent.pat_acct_num,
    pat_contact_consent.pat_contact_consent_ind,
    pat_contact_consent.vendor_contact_consent_ind,
    pat_contact_consent.exclusion_type_code,
    pat_contact_consent.vendor_notified_date,
    pat_contact_consent.source_system_code,
    pat_contact_consent.dw_last_update_date_time
  FROM
    {{ params.param_pf_base_views_dataset_name }}.pat_contact_consent
;
