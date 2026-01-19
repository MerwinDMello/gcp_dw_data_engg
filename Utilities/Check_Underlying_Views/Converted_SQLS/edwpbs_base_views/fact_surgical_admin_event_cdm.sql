-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_surgical_admin_event_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_surgical_admin_event_cdm AS SELECT
    ROUND(a.srg_admn_ev_sk, 0, 'ROUND_HALF_EVEN') AS surgical_admin_event_sk,
    ROUND(a.srg_case_sk, 0, 'ROUND_HALF_EVEN') AS surgical_case_sk,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    a.company_code AS company_code,
    a.coid AS coid,
    a.vld_fr_ts AS valid_from_date_time,
    a.ev_ts AS surgical_event_date_time,
    ROUND(a.ev_val, 4, 'ROUND_HALF_EVEN') AS surgical_event_value,
    a.ev_txt AS surgical_event_desc,
    a.ev_type_cd AS surgical_event_type_code,
    a.ev_user1_cd AS surgical_event_user_1_code,
    a.ev_user2_cd AS surgical_event_user_2_code,
    a.ev_lo_cd AS surgical_event_location_code,
    a.dely_rsn_cd AS surgical_delay_reason_code,
    a.dely_mnt AS surgical_delay_minutes,
    a.lo_type_cd AS surgical_location_type_code,
    a.trnsfr_methd_cd AS transfer_method_code,
    a.src_sys_ref_cd AS source_system_txt,
    a.dw_insrt_ts AS dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.srg_admn_ev AS a
  WHERE a.vld_to_ts = datetime(TIMESTAMP '9999-12-31 00:00:00', 'US/Central')
;
