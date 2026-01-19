-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/clinical_patient_transfer.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.clinical_patient_transfer AS SELECT
    clinical_patient_transfer.patient_dw_id,
    clinical_patient_transfer.transfer_seq,
    clinical_patient_transfer.coid,
    clinical_patient_transfer.company_code,
    clinical_patient_transfer.pat_acct_num,
    clinical_patient_transfer.transfer_eff_from_date,
    clinical_patient_transfer.transfer_eff_from_time,
    clinical_patient_transfer.transfer_eff_from_date_time,
    clinical_patient_transfer.location_mnemonic_cs,
    clinical_patient_transfer.transfer_eff_to_date,
    clinical_patient_transfer.transfer_eff_to_time,
    clinical_patient_transfer.transfer_eff_to_date_time,
    clinical_patient_transfer.facility_mnemonic_cs,
    clinical_patient_transfer.network_mnemonic_cs,
    clinical_patient_transfer.clinical_system_module_code,
    clinical_patient_transfer.source_system_code,
    clinical_patient_transfer.dw_last_update_date_time,
    clinical_patient_transfer.active_dw_ind
  FROM
    `hca-hin-dev-cur-parallon`.edwcl_base_views.clinical_patient_transfer
;
