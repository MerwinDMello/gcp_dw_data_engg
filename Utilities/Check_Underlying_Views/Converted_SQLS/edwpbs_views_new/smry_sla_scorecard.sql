-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_sla_scorecard.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_sla_scorecard AS SELECT
    smry_sla_scorecard.service_type_name,
    smry_sla_scorecard.fact_lvl_code,
    smry_sla_scorecard.month_id,
    smry_sla_scorecard.corporate_code,
    smry_sla_scorecard.metric_code,
    smry_sla_scorecard.parent_code,
    smry_sla_scorecard.child_code,
    smry_sla_scorecard.metric_actual_value,
    smry_sla_scorecard.result_ind,
    smry_sla_scorecard.change_date,
    smry_sla_scorecard.user_id,
    smry_sla_scorecard.dw_last_update_date_time,
    smry_sla_scorecard.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_sla_scorecard
;
