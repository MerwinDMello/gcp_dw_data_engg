-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/facility_dimension.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_dimension AS SELECT
    facility_dimension.unit_num,
    facility_dimension.company_code,
    facility_dimension.coid,
    facility_dimension.coid_name,
    facility_dimension.c_level,
    facility_dimension.corporate_name,
    facility_dimension.s_level,
    facility_dimension.sector_name,
    facility_dimension.b_level,
    facility_dimension.group_name,
    facility_dimension.r_level,
    facility_dimension.division_name,
    facility_dimension.d_level,
    facility_dimension.market_name,
    facility_dimension.f_level,
    facility_dimension.cons_facility_name,
    facility_dimension.lob_code,
    facility_dimension.lob_name,
    facility_dimension.sub_lob_code,
    facility_dimension.sub_lob_name,
    facility_dimension.state_code,
    facility_dimension.pas_id_current,
    facility_dimension.pas_current_name,
    facility_dimension.pas_id_future,
    facility_dimension.pas_future_name,
    facility_dimension.summary_7_member_ind,
    facility_dimension.summary_8_member_ind,
    facility_dimension.summary_phys_svc_member_ind,
    facility_dimension.summary_asd_member_ind,
    facility_dimension.summary_imaging_member_ind,
    facility_dimension.summary_oncology_member_ind,
    facility_dimension.summary_cath_lab_member_ind,
    facility_dimension.summary_intl_member_ind,
    facility_dimension.summary_other_member_ind,
    facility_dimension.pas_coid,
    facility_dimension.pas_status,
    facility_dimension.company_code_operations,
    facility_dimension.osg_pas_ind,
    facility_dimension.abs_facility_member_ind,
    facility_dimension.abl_facility_member_ind,
    facility_dimension.intl_pmis_member_ind,
    facility_dimension.hsc_member_ind
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.facility_dimension
;
