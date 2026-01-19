-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_appeal_root_cause_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_appeal_root_cause_dim AS SELECT
    eis_appeal_root_cause_dim.appeal_root_cause_sid,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01_alias,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01a,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01a_alias,
    eis_appeal_root_cause_dim.appeal_root_cause_gen02,
    eis_appeal_root_cause_dim.appeal_root_cause_gen02_alias,
    eis_appeal_root_cause_dim.appeal_root_cause_gen03,
    eis_appeal_root_cause_dim.appeal_root_cause_gen03_alias
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.eis_appeal_root_cause_dim
;
