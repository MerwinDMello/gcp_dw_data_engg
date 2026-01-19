-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_ce_location_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_ce_location_cdm AS SELECT
    a.patient_dw_id,
    a.location_transfer_seq_num,
    a.company_code,
    a.coid,
    a.pat_acct_num,
    a.clinical_location_mnemonic_cs,
    a.visit_start_date_time,
    a.visit_end_date_time,
    a.ccu_location_ind,
    a.ccu_los_amt,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.fact_ce_location AS a
;
