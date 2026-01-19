-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/clinical_patient_med_list_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.clinical_patient_med_list_cdm AS SELECT
    clinical_patient_med_list.patient_dw_id,
    clinical_patient_med_list.med_list_urn,
    clinical_patient_med_list.med_list_seq,
    clinical_patient_med_list.clinical_systems_module_code,
    clinical_patient_med_list.audit_event_seconds_amt,
    clinical_patient_med_list.company_code,
    clinical_patient_med_list.coid,
    clinical_patient_med_list.pat_acct_num,
    clinical_patient_med_list.clinical_phrm_mnem_cs,
    clinical_patient_med_list.clinical_phrm_trade_name,
    clinical_patient_med_list.no_known_meds_ind,
    clinical_patient_med_list.rx_num,
    clinical_patient_med_list.rx_dose_text,
    clinical_patient_med_list.signatura_mnem_cs,
    clinical_patient_med_list.rx_route_mnem_cs,
    clinical_patient_med_list.rx_type,
    clinical_patient_med_list.rx_include_ind,
    clinical_patient_med_list.rx_dispense_use_text,
    clinical_patient_med_list.rx_refill_qty,
    clinical_patient_med_list.rx_source_code,
    clinical_patient_med_list.prescriber_mnem_cs,
    clinical_patient_med_list.med_rec_action_user_mnem_cs,
    clinical_patient_med_list.update_user_mnem_cs,
    clinical_patient_med_list.update_date_time,
    clinical_patient_med_list.med_status_code,
    clinical_patient_med_list.rx_action_text,
    clinical_patient_med_list.rx_unit_text,
    clinical_patient_med_list.rx_frequency_text,
    clinical_patient_med_list.facility_mnemonic_cs,
    clinical_patient_med_list.network_mnemonic_cs,
    clinical_patient_med_list.pk_print_ind,
    clinical_patient_med_list.pk_print_date_time,
    clinical_patient_med_list.entered_date_time,
    clinical_patient_med_list.print_status_date_time,
    clinical_patient_med_list.last_called_in_date_time,
    clinical_patient_med_list.first_erx_submit_date_time,
    clinical_patient_med_list.first_fax_date_time,
    clinical_patient_med_list.electronic_signature_text,
    clinical_patient_med_list.source_system_code,
    clinical_patient_med_list.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.clinical_patient_med_list
;
