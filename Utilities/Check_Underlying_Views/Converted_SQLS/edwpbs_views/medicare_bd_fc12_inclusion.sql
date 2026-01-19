-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/medicare_bd_fc12_inclusion.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.medicare_bd_fc12_inclusion AS SELECT
    armap_recoveries.unit_num,
    armap_recoveries.coid AS iplan_id,
    substr(ltrim(regexp_replace(format('%#20.3f', armap_recoveries.recovery_amt), r'^( *?)(-)?0(\..*)', r'\2\3')), 1, 2) AS financial_class
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.armap_recoveries
  WHERE upper(armap_recoveries.agency_name) = 'MEDICARE_BD_FC12_INCLUSION'
;
