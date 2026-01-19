-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payor_spcl_recovery_import_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_spcl_recovery_import_dtl AS SELECT
    payor_spcl_recovery_import_dtl.pe_date,
    payor_spcl_recovery_import_dtl.patient_dw_id,
    payor_spcl_recovery_import_dtl.appl_team_flag,
    payor_spcl_recovery_import_dtl.iplan_id,
    payor_spcl_recovery_import_dtl.coid,
    payor_spcl_recovery_import_dtl.company_code,
    payor_spcl_recovery_import_dtl.pat_acct_num,
    payor_spcl_recovery_import_dtl.unit_num,
    payor_spcl_recovery_import_dtl.recovery_amt,
    payor_spcl_recovery_import_dtl.source_system_code,
    payor_spcl_recovery_import_dtl.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.payor_spcl_recovery_import_dtl
;
