-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/facility_dimension_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.facility_dimension_eom AS SELECT
    a.company_code,
    a.coid,
    a.pe_date,
    a.unit_num,
    a.coid_name,
    a.c_level,
    a.corporate_name,
    a.s_level,
    a.sector_name,
    a.b_level,
    a.group_name,
    a.r_level,
    a.division_name,
    a.d_level,
    a.market_name,
    a.f_level,
    a.cons_facility_name,
    a.lob_code,
    a.lob_name,
    a.sub_line_of_business_code,
    a.sub_line_of_business_name,
    a.state_code,
    a.pas_id_current,
    a.pas_current_name,
    a.pas_id_future,
    a.pas_future_name,
    a.summary_7_member_ind,
    a.summary_8_member_ind,
    a.summary_phys_svc_member_ind,
    a.summary_asd_member_ind,
    a.summary_imaging_member_ind,
    a.summary_oncology_member_ind,
    a.summary_cath_lab_member_ind,
    a.summary_intl_member_ind,
    a.summary_other_member_ind,
    a.pas_coid,
    a.pas_status,
    a.company_code_operations,
    a.osg_pas_ind,
    a.abs_facility_member_ind,
    a.abl_facility_member_ind,
    a.intl_pmis_member_ind,
    a.hsc_member_ind,
    a.source_system_code,
    a.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_dimension_eom AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
