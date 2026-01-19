-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/armap_clean_claim_pcnt.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.armap_clean_claim_pcnt AS SELECT
    armap_clean_claim_pcnt.rptg_period,
    armap_clean_claim_pcnt.unit_num,
    armap_clean_claim_pcnt.coid,
    armap_clean_claim_pcnt.ssc_total_claims,
    armap_clean_claim_pcnt.msc_total_claims,
    armap_clean_claim_pcnt.ssc_clean_claims,
    armap_clean_claim_pcnt.msc_clean_claims
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.armap_clean_claim_pcnt
;
