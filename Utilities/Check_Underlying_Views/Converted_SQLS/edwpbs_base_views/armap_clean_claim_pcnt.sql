-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
