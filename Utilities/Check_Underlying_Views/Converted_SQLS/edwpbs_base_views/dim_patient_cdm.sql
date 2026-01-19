-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_patient_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_patient_cdm AS SELECT
    ROUND(ptnt_dtl.role_plyr_sk, 0, 'ROUND_HALF_EVEN') AS patient_sk,
    ROUND(ptnt_dtl.role_plyr_dw_id, 0, 'ROUND_HALF_EVEN') AS role_plyr_dw_id,
    ptnt_dtl.umpi_txt,
    ptnt_dtl.addr_line1_desc AS address_line1_desc,
    ptnt_dtl.addr_line2_desc AS address_line2_desc,
    ptnt_dtl.city_nm AS city_name,
    ptnt_dtl.cntry_nm AS country_name,
    ptnt_dtl.pstl_cd AS postal_code,
    ptnt_dtl.st_prv_cd AS state_code,
    ptnt_dtl.prfx_nm AS prefix_name,
    ptnt_dtl.frst_nm AS first_name,
    ptnt_dtl.mid_nm AS middle_name,
    ptnt_dtl.lst_nm AS last_name,
    ptnt_dtl.sfx_nm AS suffix_name,
    ptnt_dtl.fl_nm AS full_name,
    ptnt_dtl.deg_nm AS degree_name,
    ptnt_dtl.brth_ts AS birth_date_time,
    ptnt_dtl.dth_ts AS death_date_time,
    ptnt_dtl.lang_cd_sk AS primary_language_sk,
    ptnt_dtl.liv_sts_ref_cd AS living_status_code,
    ptnt_dtl.mrtl_sts_ref_cd AS marital_status_code,
    ptnt_dtl.mthr_maid_nm AS mother_name,
    ptnt_dtl.registn_ts AS registration_date_time,
    ptnt_dtl.relg_cd_sk AS religion_sk,
    ptnt_dtl.sex_ref_cd AS sex_code,
    ptnt_dtl.vip_ind,
    ptnt_dtl.race_cd_sk AS race_sk,
    ptnt_dtl.hme_ph_nbr AS home_phone_num,
    ptnt_dtl.bsns_ph_nbr AS business_phone_num,
    ptnt_dtl.mobl_ph_nbr AS mobile_phone_num,
    ptnt_dtl.dw_insrt_ts AS dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.ptnt_dtl
  WHERE ptnt_dtl.vld_to_ts = datetime(TIMESTAMP '9999-12-31 00:00:00', 'US/Central')
;
