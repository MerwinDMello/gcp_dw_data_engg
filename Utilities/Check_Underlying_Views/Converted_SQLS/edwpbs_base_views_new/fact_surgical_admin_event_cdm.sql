-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_surgical_admin_event_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_surgical_admin_event_cdm AS SELECT
    a.srg_admn_ev_sk AS surgical_admin_event_sk,
    a.srg_case_sk AS surgical_case_sk,
    a.patient_dw_id AS patient_dw_id,
    a.company_code AS company_code,
    a.coid AS coid,
    a.vld_fr_ts AS valid_from_date_time,
    a.ev_ts AS surgical_event_date_time,
    a.ev_val AS surgical_event_value,
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
  WHERE a.vld_to_ts = '9999-12-31 00:00:00'
;
