-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/pdoc_order_entry_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.pdoc_order_entry_cdm AS SELECT
    pdoc_order_entry.coid,
    pdoc_order_entry.company_code,
    pdoc_order_entry.oe_dept_num,
    pdoc_order_entry.oe_id_text,
    pdoc_order_entry.active_ind,
    pdoc_order_entry.oe_desc,
    pdoc_order_entry.oe_final_status_code,
    pdoc_order_entry.pci_status_code,
    pdoc_order_entry.bill_on_status_code,
    pdoc_order_entry.report_label_code,
    pdoc_order_entry.section_title_ind,
    pdoc_order_entry.report_title_ind,
    pdoc_order_entry.standard_trailer_ind,
    pdoc_order_entry.page_numbering_ind,
    pdoc_order_entry.require_diction_date_time_ind,
    pdoc_order_entry.default_status_code,
    pdoc_order_entry.contiuous_ind,
    pdoc_order_entry.electronically_sign_pci_ind,
    pdoc_order_entry.format_type_code,
    pdoc_order_entry.last_update_date,
    pdoc_order_entry.autoprint_status_code,
    pdoc_order_entry.available_pdoc_ind,
    pdoc_order_entry.order_required_ind,
    pdoc_order_entry.send_outbound_ov_ind,
    pdoc_order_entry.temporary_patient_ind,
    pdoc_order_entry.duplicate_report_hours_num,
    pdoc_order_entry.screen_edit_pci_num,
    pdoc_order_entry.enable_mri_ind,
    pdoc_order_entry.patient_portal_status_code,
    pdoc_order_entry.portal_delay_hour_num,
    pdoc_order_entry.medical_record_form_id_text,
    pdoc_order_entry.electronic_form_id_text,
    pdoc_order_entry.abnormal_query_id_text,
    pdoc_order_entry.clinic_alert_primary_id_text,
    pdoc_order_entry.clinic_alert_secondary_id_text,
    pdoc_order_entry.print_interfaced_report_text,
    pdoc_order_entry.emr_report_id_text,
    pdoc_order_entry.emr_report_proc_name,
    pdoc_order_entry.source_system_code,
    pdoc_order_entry.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.pdoc_order_entry
;
