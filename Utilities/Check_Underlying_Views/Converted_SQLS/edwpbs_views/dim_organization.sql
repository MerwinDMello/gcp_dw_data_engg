-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_organization AS SELECT
    syslib.ascii(substr(b.customer_code, 1, 1)) * 1000 + syslib.ascii(substr(b.customer_code, 2, 1)) * 10 + syslib.ascii(substr(b.customer_code, 3, 1)) AS customer_sid,
    syslib.ascii(a.company_code) AS company_code_sid,
    CASE
       a.unit_num
      WHEN '' THEN 0
      ELSE CAST(a.unit_num as INT64)
    END AS unit_num_sid,
    CASE
       a.coid
      WHEN '' THEN 0
      ELSE CAST(a.coid as INT64)
    END AS coid_sid,
    CASE
       substr(b.group_code, 2, 5)
      WHEN '' THEN 0
      ELSE CAST(substr(b.group_code, 2, 5) as INT64)
    END AS group_sid,
    CASE
       substr(b.division_code, 2, 5)
      WHEN '' THEN 0
      ELSE CAST(substr(b.division_code, 2, 5) as INT64)
    END AS division_sid,
    CASE
       substr(b.market_code, 2, 5)
      WHEN '' THEN 0
      ELSE CAST(substr(b.market_code, 2, 5) as INT64)
    END AS market_sid,
    b.ssc_code,
    b.customer_code,
    b.customer_short_name,
    b.company_code,
    b.unit_num,
    b.coid,
    b.coid_name,
    b.group_name,
    b.division_name,
    b.market_name,
    CASE
       upper(b.ssc_alias_name)
      WHEN 'NASHVILLE SSC' THEN 8942
      WHEN 'NASHVILLE WEST SSC' THEN 8591
      WHEN 'DALLAS SSC' THEN 8950
      WHEN 'CINCINNATI SSC' THEN 26600
      WHEN 'HOUSTON SSC' THEN 8948
      WHEN 'ORANGE PARK SSC' THEN 8945
      WHEN 'RICHMOND SSC' THEN 8648
      WHEN 'SAN ANTONIO SSC' THEN 8949
      WHEN 'TAMPA SSC' THEN 8947
      WHEN 'UNKNOWN' THEN 0
      ELSE 999
    END AS ssc_coid,
    b.ssc_name,
    CASE
       upper(b.ssc_coid)
      WHEN '08591' THEN 'Nashville HSC'
      WHEN '08950' THEN 'Dallas HSC'
      WHEN '08948' THEN 'Houston HSC'
      WHEN '08942' THEN 'Nashville HSC'
      WHEN '08945' THEN 'Orange Park HSC'
      WHEN '08648' THEN 'Richmond HSC'
      WHEN '08949' THEN 'San Antonio HSC'
      WHEN '08947' THEN 'Tampa HSC'
      ELSE CAST(NULL as STRING)
    END AS hsc_alias_name,
    CASE
       upper(c.cpc_coid)
      WHEN '25402' THEN 'Nashville CPC'
      WHEN '25401' THEN 'Houston CPC'
      WHEN '25400' THEN 'Orange Park SSC'
      ELSE CAST(NULL as STRING)
    END AS cpc_alias_name,
    a.lob_code,
    a.summary_7_member_ind,
    a.summary_8_member_ind,
    b.go_live_date,
    b.eff_from_date,
    b.eff_to_date,
    b.ssc_alias_code,
    b.division_alias_code,
    b.facility_state_code,
    b.f_level,
    b.partnership_ind,
    b.medicare_expansion_ind,
    b.medicaid_conversion_vendor_name AS medicaid_conv_vendor_name,
    b.facility_close_date,
    b.his_vendor_name,
    b.hsc_member_ind,
    b.clear_contract_ind,
    b.client_outbound_ind,
    b.him_conversion_date,
    b.summ_days_release_date,
    b.rcps_migration_date,
    ROUND(b.sma_high_dollar_threshold_amt, 3, 'ROUND_HALF_EVEN') AS sma_high_dollar_threshold_amt,
    b.ssc_alias_name,
    b.corporate_name,
    b.customer_name,
    ROUND(b.discrepancy_threshold_amt, 3, 'ROUND_HALF_EVEN') AS discrepancy_threshold_amt,
    b.division_alias_name,
    b.division_code,
    b.facility_name,
    b.group_alias_name,
    b.group_code,
    b.market_alias_name,
    b.market_code,
    d.pas_status,
    d.abs_facility_member_ind,
    d.abl_facility_member_ind,
    b.facility_mnemonic,
    CASE
      WHEN upper(b.ssc_coid) IN(
        '08942', '08591'
      )
       AND (upper(b.company_code) = 'H'
       OR upper(b.customer_code) IN(
        'PDM', 'ADV'
      )) THEN '63333'
      WHEN upper(b.ssc_coid) IN(
        '08942', '08591'
      ) THEN '61111'
      WHEN upper(b.ssc_coid) IN(
        '08950', '08948', '08949'
      ) THEN '62222'
      WHEN upper(b.ssc_coid) IN(
        '08945', '08947', '25464'
      )
       OR upper(b.coid) IN(
        '02781', '05225', '05284', '05285', '05450', '16120', '16150', '16406', '16776', '16830', '16881', '18045', '18230', '18235', '18500', '76009', '02740', '05359'
      ) THEN '61111'
      WHEN upper(b.ssc_coid) IN(
        '08648'
      )
       AND upper(b.coid) NOT IN(
        '02781', '05225', '05284', '05285', '05450', '16120', '16150', '16406', '16776', '16830', '16881', '18045', '18230', '18235', '18500', '76009', '02740', '05359'
      ) THEN '64444'
      ELSE '00000'
    END AS consolidated_ssc_num,
    CASE
      WHEN upper(b.ssc_coid) IN(
        '08942', '08591'
      )
       AND (upper(b.company_code) = 'H'
       OR upper(b.customer_code) IN(
        'PDM', 'ADV'
      )) THEN 'Nashville SSC'
      WHEN upper(b.ssc_coid) IN(
        '08942', '08591'
      ) THEN 'Florida SSC'
      WHEN upper(b.ssc_coid) IN(
        '08950', '08948', '08949'
      ) THEN 'Texas SSC'
      WHEN upper(b.ssc_coid) IN(
        '08945', '08947', '25464'
      )
       OR upper(b.coid) IN(
        '02781', '05225', '05284', '05285', '05450', '16120', '16150', '16406', '16776', '16830', '16881', '18045', '18230', '18235', '18500', '76009', '02740', '05359'
      ) THEN 'Florida SSC'
      WHEN upper(b.ssc_coid) IN(
        '08648'
      )
       AND upper(b.coid) NOT IN(
        '02781', '05225', '05284', '05285', '05450', '16120', '16150', '16406', '16776', '16830', '16881', '18045', '18230', '18235', '18500', '76009', '02740', '05359'
      ) THEN 'Richmond SSC'
      ELSE 'Unknown'
    END AS consolidated_ssc_alias_name,
    CASE
       e.schema_id
      WHEN 1 THEN 'P1'
      WHEN 3 THEN 'P2'
      ELSE 'Other'
    END AS concuity_schema,
    e.org_id
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_rcm_organization AS b
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_dimension AS a ON upper(a.coid) = upper(b.coid)
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.bi_cpc_entity_hierarchy AS c ON upper(c.coid) = upper(b.coid)
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_dimension_eom AS d ON upper(d.coid) = upper(b.coid)
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwra_views.ref_cc_org_structure AS e ON upper(e.coid) = upper(b.coid)
     AND upper(e.company_code) = upper(b.company_code)
;
