-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/physician_credential_subset.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.physician_credential_subset AS SELECT
    hcp.hcp_dw_id,
    hcp.hcp_full_name,
    hcp.hcp_last_name,
    hcp.hcp_first_name,
    hcp.hcp_middle_name,
    hcp.hcp_npi,
    hcp.upin,
    rms.med_spcl_desc,
    c.coid AS credentialing_coid,
    ff.market_name,
    fs.city_name,
    fs.state_code,
    c.entity_sid AS credenting_entity_sid,
    ent.entity_desc AS credentialing_entity_desc,
    a.asgn_desc AS credentialing_type
  FROM
    {{ params.param_cr_base_views_dataset_name }}.hcp
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.credentialing_asgn AS c ON hcp.hcp_dw_id = c.hcp_dw_id
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.junc_hcp_specialty AS jhs ON hcp.hcp_dw_id = jhs.hcp_dw_id
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_medical_specialty AS rms ON jhs.med_spcl_dw_id = rms.med_spcl_dw_id
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.fact_facility AS ff ON c.coid = ff.coid
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.facility_address AS fs ON c.coid = fs.coid
     AND upper(fs.address_type_code) = 'PH'
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.hcp_facility_assignment AS fa ON c.hcp_fac_asgn_sid = fa.hcp_fac_asgn_sid
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_assignment AS a ON fa.asgn_sid = a.asgn_sid
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_entity AS ent ON c.entity_sid = ent.entity_sid
     AND ent.entity_type_sid IN(
      41852, 11453
    )
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON b.co_id = fa.coid
     AND b.company_code = fa.company_code
     AND b.user_id = session_user()
  WHERE hcp.hcp_npi IS NOT NULL
   AND upper(c.active_dw_ind) = 'Y'
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
;
