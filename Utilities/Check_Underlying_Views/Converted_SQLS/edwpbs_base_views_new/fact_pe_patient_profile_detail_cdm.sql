-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_pe_patient_profile_detail_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_pe_patient_profile_detail_cdm AS SELECT
    a.patient_dw_id,
    a.sort_seq_num,
    a.coid,
    a.company_code,
    a.pat_acct_num,
    a.record_type,
    a.record_code,
    a.record_desc,
    a.row_message_text,
    a.complication_message_text,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.fact_pe_patient_profile_detail AS a
;
