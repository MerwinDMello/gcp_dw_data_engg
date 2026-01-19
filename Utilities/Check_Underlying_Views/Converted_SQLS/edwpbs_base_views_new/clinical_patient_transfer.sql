-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
