-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/rxnorm_ndc_xwalk_cl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.rxnorm_ndc_xwalk_cl AS SELECT
    rxnorm_ndc_xwalk.rxnorm_id,
    rxnorm_ndc_xwalk.ndc,
    rxnorm_ndc_xwalk.ndc_num,
    rxnorm_ndc_xwalk.reference_map_desc,
    rxnorm_ndc_xwalk.source_system_code,
    rxnorm_ndc_xwalk.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcl_base_views.rxnorm_ndc_xwalk
;
