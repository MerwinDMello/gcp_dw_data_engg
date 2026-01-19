CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.physician_credential_subset AS SELECT
    hcp.hcp_dw_id,
    max(hcp.hcp_full_name) AS hcp_full_name,
    max(hcp.hcp_last_name) AS hcp_last_name,
    max(hcp.hcp_first_name) AS hcp_first_name,
    max(hcp.hcp_middle_name) AS hcp_middle_name,
    hcp.hcp_npi,
    hcp.upin,
    max(rms.med_spcl_desc) AS med_spcl_desc,
    c.coid AS credentialing_coid,
    max(ff.market_name) AS market_name,
    max(fs.city_name) AS city_name,
    fs.state_code,
    c.entity_sid AS credenting_entity_sid,
    max(ent.entity_desc) AS credentialing_entity_desc,
    max(a.asgn_desc) AS credentialing_type
  FROM
    {{ params.param_cr_base_views_dataset_name }}.hcp
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.credentialing_asgn AS c ON hcp.hcp_dw_id = c.hcp_dw_id
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.junc_hcp_specialty AS jhs ON hcp.hcp_dw_id = jhs.hcp_dw_id
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_medical_specialty AS rms ON jhs.med_spcl_dw_id = rms.med_spcl_dw_id
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.fact_facility AS ff ON rtrim(c.coid) = rtrim(ff.coid)
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.facility_address AS fs ON rtrim(c.coid) = rtrim(fs.coid)
     AND rtrim(fs.address_type_code) = 'PH'
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.hcp_facility_assignment AS fa ON c.hcp_fac_asgn_sid = fa.hcp_fac_asgn_sid
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_assignment AS a ON fa.asgn_sid = a.asgn_sid
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_entity AS ent ON c.entity_sid = ent.entity_sid
     AND ent.entity_type_sid IN(
      41852, 11453
    )
  WHERE hcp.hcp_npi IS NOT NULL
   AND rtrim(c.active_dw_ind) = 'Y'
  GROUP BY 1, upper(hcp.hcp_full_name), upper(hcp.hcp_last_name), upper(hcp.hcp_first_name), upper(hcp.hcp_middle_name), 6, 7, upper(rms.med_spcl_desc), 9, upper(ff.market_name), upper(fs.city_name), 12, 13, upper(ent.entity_desc), upper(a.asgn_desc)
;
