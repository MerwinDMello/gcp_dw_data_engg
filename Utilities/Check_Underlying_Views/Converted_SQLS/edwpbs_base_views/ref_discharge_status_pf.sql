-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_discharge_status_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_discharge_status_pf AS SELECT
    ref_discharge_status.discharge_status_code,
    ref_discharge_status.discharge_status_code_desc,
    ref_discharge_status.eff_from_date,
    ref_discharge_status.eff_to_date,
    ref_discharge_status.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.ref_discharge_status
;
