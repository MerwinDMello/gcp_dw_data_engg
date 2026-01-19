-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
