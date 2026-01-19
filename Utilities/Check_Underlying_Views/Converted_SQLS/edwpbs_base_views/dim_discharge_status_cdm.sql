-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_discharge_status_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_discharge_status_cdm AS SELECT
    a.discharge_status_sk,
    a.discharge_status_code,
    a.discharge_status_desc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.dim_discharge_status AS a
;
