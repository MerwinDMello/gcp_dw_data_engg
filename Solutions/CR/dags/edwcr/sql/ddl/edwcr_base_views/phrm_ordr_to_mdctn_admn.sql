CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.phrm_ordr_to_mdctn_admn AS SELECT
    phrm_ordr_to_mdctn_admn.phrm_ordr_sk,
    phrm_ordr_to_mdctn_admn.mdctn_admn_sk,
    phrm_ordr_to_mdctn_admn.rl_type_ref_cd,
    phrm_ordr_to_mdctn_admn.vld_fr_ts,
    phrm_ordr_to_mdctn_admn.patient_dw_id,
    phrm_ordr_to_mdctn_admn.company_code,
    phrm_ordr_to_mdctn_admn.coid,
    phrm_ordr_to_mdctn_admn.vld_to_ts,
    phrm_ordr_to_mdctn_admn.phrm_ordr_type_ref_cd,
    phrm_ordr_to_mdctn_admn.mdctn_admn_type_ref_cd,
    phrm_ordr_to_mdctn_admn.effv_fr_dt,
    phrm_ordr_to_mdctn_admn.effv_to_dt,
    phrm_ordr_to_mdctn_admn.subjt_ssukt,
    phrm_ordr_to_mdctn_admn.obj_ssukt,
    phrm_ordr_to_mdctn_admn.dw_insrt_ts
  FROM
    {{ params.param_auth_base_views_dataset_name }}.phrm_ordr_to_mdctn_admn
;
