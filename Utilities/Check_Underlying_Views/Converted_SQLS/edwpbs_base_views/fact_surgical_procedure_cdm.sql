-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_surgical_procedure_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_surgical_procedure_cdm AS SELECT
    ROUND(a.srg_prcdr_sk, 0, 'ROUND_HALF_EVEN') AS surgical_procedure_sk,
    CASE
      WHEN a.actl_prcdr_cd_sk = -8 THEN a.prpsd_prcdr_cd_sk
      ELSE a.actl_prcdr_cd_sk
    END AS surgical_procedure_code_sk,
    ROUND(a.srg_case_sk, 0, 'ROUND_HALF_EVEN') AS surgical_case_sk,
    a.bdy_site_cd_sk AS body_site_sk,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    a.company_code AS company_code,
    a.coid AS coid,
    a.vld_fr_ts AS valid_from_date_time,
    CASE
      WHEN a.actl_prcdr_cd_sk = -8 THEN a.srg_prcdr_txt
      ELSE a.actl_prcdr_cd_desc
    END AS surgical_procedure_txt,
    a.prcdr_type_cd AS surgical_procedure_type_code,
    a.prmy_prcdr_ind AS primary_procedure_ind,
    a.ltrlty_ref_cd AS surgical_laterality_code,
    a.svrty_cd AS surgical_severity_code,
    a.lo_type_cd AS surgical_location_type_code,
    a.wnd_cd AS surgical_wound_code,
    a.dvc_sz AS surgical_device_size,
    a.qty AS surgical_quantity,
    a.usr_cd AS user_code,
    a.src_sys_ref_cd AS source_system_txt,
    a.dw_insrt_ts AS last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.srg_prcdr_dtl AS a
  WHERE a.vld_to_ts = datetime(TIMESTAMP '9999-12-31 00:00:00', 'US/Central')
;
