-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
