-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/bi_cpc_entity_hierarchy.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.bi_cpc_entity_hierarchy AS SELECT
    ffc.company_code AS company_code,
    ffcorp.coid AS corporate_coid,
    ffcorp.coid_name AS corporate_name,
    ffcpc.coid AS cpc_coid,
    ffcpc.coid_name AS cpc_name,
    ffc.coid AS coid,
    ffc.coid_name AS coid_name,
    ffc.group_code AS group_code,
    ffc.group_name AS group_name,
    ffc.division_code AS division_code,
    ffc.division_name AS division_name,
    ffc.market_code AS market_code,
    ffc.market_name AS market_name
  FROM
    (
      SELECT
          max(CASE
            WHEN upper(org_unit_alt_hierarchy.immediate_parent_ind) = 'E' THEN org_unit_alt_hierarchy.parent_coid
          END) AS corporate_coid,
          max(CASE
            WHEN upper(org_unit_alt_hierarchy.immediate_parent_ind) = 'I' THEN org_unit_alt_hierarchy.parent_coid
          END) AS cpc_coid,
          org_unit_alt_hierarchy.member_coid AS coid
        FROM
          `hca-hin-dev-cur-parallon`.edw_pub_views.org_unit_alt_hierarchy
        WHERE upper(org_unit_alt_hierarchy.alt_hierarchy_type_code) = 'CDR'
         AND upper(org_unit_alt_hierarchy.type_of_parent_code) = 'C'
         AND org_unit_alt_hierarchy.acct_period_date = (
          SELECT
              max(org_unit_alt_hierarchy_0.acct_period_date)
            FROM
              `hca-hin-dev-cur-parallon`.edw_pub_views.org_unit_alt_hierarchy AS org_unit_alt_hierarchy_1
            WHERE upper(org_unit_alt_hierarchy_2.alt_hierarchy_type_code) = 'CDR'
        )
        GROUP BY 3
        HAVING max(CASE
          WHEN upper(org_unit_alt_hierarchy.immediate_parent_ind) = 'E' THEN org_unit_alt_hierarchy.parent_coid
        END) IS NOT NULL
    ) AS ouah
    INNER JOIN `hca-hin-dev-cur-parallon`.edw_pub_views.fact_facility AS ffc ON ffc.coid = ouah.coid
    INNER JOIN `hca-hin-dev-cur-parallon`.edw_pub_views.fact_facility AS ffcorp ON ffcorp.coid = ouah.corporate_coid
    INNER JOIN `hca-hin-dev-cur-parallon`.edw_pub_views.fact_facility AS ffcpc ON ffcpc.coid = ouah.cpc_coid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON upper(ffc.coid) = upper(b.co_id)
     AND b.user_id = session_user()
;
