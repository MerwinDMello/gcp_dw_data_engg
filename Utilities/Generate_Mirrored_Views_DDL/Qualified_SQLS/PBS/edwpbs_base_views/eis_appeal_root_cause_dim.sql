-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_appeal_root_cause_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.eis_appeal_root_cause_dim AS SELECT
    ROUND(eis_appeal_root_cause_dim.appeal_root_cause_sid, 0, 'ROUND_HALF_EVEN') AS appeal_root_cause_sid,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01_alias,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01a,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01a_alias,
    eis_appeal_root_cause_dim.appeal_root_cause_gen02,
    eis_appeal_root_cause_dim.appeal_root_cause_gen02_alias,
    eis_appeal_root_cause_dim.appeal_root_cause_gen03,
    eis_appeal_root_cause_dim.appeal_root_cause_gen03_alias
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.eis_appeal_root_cause_dim
;
