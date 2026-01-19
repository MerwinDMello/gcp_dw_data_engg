-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_rcm_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_organization AS SELECT
    a.company_code,
    a.coid,
    a.customer_code,
    a.customer_short_name,
    a.customer_name,
    a.ssc_code,
    a.unit_num,
    a.facility_mnemonic,
    a.group_code,
    a.division_code,
    a.market_code,
    a.f_level,
    a.partnership_ind,
    a.go_live_date,
    a.eff_from_date,
    a.eff_to_date,
    a.ssc_alias_code,
    a.division_alias_code,
    a.ssc_name,
    a.ssc_alias_name,
    CASE
      WHEN a.ssc_coid IN(
        '08942', '08591'
      )
       AND (upper(a.company_code) = 'H'
       OR a.customer_code IN(
        'PDM', 'ADV'
      )) THEN 'Nashville SSC'
      WHEN a.ssc_coid IN(
        '08942', '08591'
      ) THEN 'Florida SSC'
      WHEN a.ssc_coid IN(
        '08950', '08948', '08949'
      ) THEN 'Texas SSC'
      WHEN a.ssc_coid IN(
        '08945', '08947', '25464'
      )
       OR a.coid IN(
        '02781', '05225', '05284', '05285', '05450', '16120', '16150', '16406', '16776', '16830', '16881', '18045', '18230', '18235', '18500', '76009', '02740', '05359'
      ) THEN 'Florida SSC'
      WHEN a.ssc_coid IN(
        '08648'
      )
       AND a.coid NOT IN(
        '02781', '05225', '05284', '05285', '05450', '16120', '16150', '16406', '16776', '16830', '16881', '18045', '18230', '18235', '18500', '76009', '02740', '05359'
      ) THEN 'Richmond SSC'
      WHEN upper(a.customer_code) = 'HHH' THEN 'Specialty Services'
      ELSE 'Unknown'
    END AS consolidated_ssc_alias_name,
    a.corporate_name,
    a.group_name,
    a.market_name,
    a.division_name,
    a.group_alias_name,
    a.market_alias_name,
    a.division_alias_name,
    a.ssc_coid,
    a.coid_name,
    CASE
      WHEN a.ssc_coid IN(
        '08942', '08591'
      )
       AND (upper(a.company_code) = 'H'
       OR a.customer_code IN(
        'PDM', 'ADV'
      )) THEN '63333'
      WHEN a.ssc_coid IN(
        '08942', '08591'
      ) THEN '61111'
      WHEN a.ssc_coid IN(
        '08950', '08948', '08949'
      ) THEN '62222'
      WHEN a.ssc_coid IN(
        '08945', '08947', '25464'
      )
       OR a.coid IN(
        '02781', '05225', '05284', '05285', '05450', '16120', '16150', '16406', '16776', '16830', '16881', '18045', '18230', '18235', '18500', '76009', '02740', '05359'
      ) THEN '61111'
      WHEN a.ssc_coid IN(
        '08648'
      )
       AND a.coid NOT IN(
        '02781', '05225', '05284', '05285', '05450', '16120', '16150', '16406', '16776', '16830', '16881', '18045', '18230', '18235', '18500', '76009', '02740', '05359'
      ) THEN '64444'
      ELSE '00000'
    END AS consolidated_ssc_num,
    a.facility_name,
    a.facility_state_code,
    a.medicare_expansion_ind,
    a.medicaid_conversion_vendor_name,
    a.facility_close_date,
    a.his_vendor_name,
    a.rcps_migration_date,
    a.discrepancy_threshold_amt,
    a.sma_high_dollar_threshold_amt,
    a.hsc_member_ind,
    a.clear_contract_ind,
    a.client_outbound_ind,
    a.him_conversion_date,
    a.summ_days_release_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_rcm_organization AS a
;
