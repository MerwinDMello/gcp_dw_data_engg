-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_Base_Views/ga_patient_physician_interaction.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ga_patient_physician_interaction AS SELECT
    patient_physician_interaction.patient_dw_id,
    patient_physician_interaction.company_code,
    patient_physician_interaction.coid,
    patient_physician_interaction.attending_phys_name,
    patient_physician_interaction.attending_phys_npi,
    patient_physician_interaction.attending_phys_fac_phys_num,
    patient_physician_interaction.attending_phys_spcl_code,
    patient_physician_interaction.attending_phys_hsptlst_ind,
    patient_physician_interaction.attending_phys_group_name,
    patient_physician_interaction.source_system_code,
    patient_physician_interaction.dw_last_update_date_time
  FROM
    {{ params.param_ga_base_views_dataset_name }}.patient_physician_interaction
;
