-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_practitioner_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_practitioner_cdm AS SELECT
    ROUND(prctnr_dtl.role_plyr_sk, 0, 'ROUND_HALF_EVEN') AS practitioner_sk,
    prctnr_dtl.spcly_cd_sk AS primary_specialty_sk,
    ROUND(prctnr_dtl.role_plyr_dw_id, 0, 'ROUND_HALF_EVEN') AS role_plyr_dw_id,
    prctnr_dtl.prfx_nm AS prefix_name,
    prctnr_dtl.frst_nm AS first_name,
    prctnr_dtl.mid_nm AS middle_name,
    prctnr_dtl.lst_nm AS last_name,
    prctnr_dtl.sfx_nm AS suffix_name,
    prctnr_dtl.deg_nm AS degree_name,
    prctnr_dtl.fl_nm AS full_name,
    prctnr_dtl.addr_line1_desc AS address_line1_desc,
    prctnr_dtl.addr_line2_desc AS address_line2_desc,
    prctnr_dtl.city_nm AS city_name,
    prctnr_dtl.st_prv_cd AS state_code,
    prctnr_dtl.pstl_cd AS postal_code,
    prctnr_dtl.cntry_nm AS country_name,
    prctnr_dtl.fax_ph_nbr AS fax_phone_num,
    prctnr_dtl.ph_nbr AS phone_num,
    prctnr_dtl.pgr_ph_nbr AS pager_phone_num,
    prctnr_dtl.prctnr_ind AS practitioner_ind,
    prctnr_dtl.eltrn_sig_ind AS electronic_sign_ind,
    prctnr_dtl.direct_ccd_eml AS direct_ccd_email,
    prctnr_dtl.actv_ind AS active_ind,
    prctnr_dtl.src_sys_ref_cd AS source_system_ref_code,
    prctnr_dtl.src_sys_unq_key_txt AS source_system_unique_key_txt,
    prctnr_dtl.vld_fr_ts AS valid_from_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.prctnr_dtl
  WHERE prctnr_dtl.vld_to_ts = datetime(TIMESTAMP '9999-12-31 00:00:00', 'US/Central')
;
