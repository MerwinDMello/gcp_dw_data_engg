-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/facility_dimension.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.facility_dimension AS SELECT
    a.unit_num,
    a.company_code,
    a.coid,
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
    a.sub_lob_code,
    a.sub_lob_name,
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
    a.cons_pas_coid,
    a.cons_pas_name
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.facility_dimension AS a
;
