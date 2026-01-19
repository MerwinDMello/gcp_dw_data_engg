-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/facility_dimension_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_dimension_eom AS SELECT
    facility_dimension_eom.company_code,
    facility_dimension_eom.coid,
    facility_dimension_eom.pe_date,
    facility_dimension_eom.unit_num,
    facility_dimension_eom.coid_name,
    facility_dimension_eom.c_level,
    facility_dimension_eom.corporate_name,
    facility_dimension_eom.s_level,
    facility_dimension_eom.sector_name,
    facility_dimension_eom.b_level,
    facility_dimension_eom.group_name,
    facility_dimension_eom.r_level,
    facility_dimension_eom.division_name,
    facility_dimension_eom.d_level,
    facility_dimension_eom.market_name,
    facility_dimension_eom.f_level,
    facility_dimension_eom.cons_facility_name,
    facility_dimension_eom.lob_code,
    facility_dimension_eom.lob_name,
    facility_dimension_eom.sub_line_of_business_code,
    facility_dimension_eom.sub_line_of_business_name,
    facility_dimension_eom.state_code,
    facility_dimension_eom.pas_id_current,
    facility_dimension_eom.pas_current_name,
    facility_dimension_eom.pas_id_future,
    facility_dimension_eom.pas_future_name,
    facility_dimension_eom.summary_7_member_ind,
    facility_dimension_eom.summary_8_member_ind,
    facility_dimension_eom.summary_phys_svc_member_ind,
    facility_dimension_eom.summary_asd_member_ind,
    facility_dimension_eom.summary_imaging_member_ind,
    facility_dimension_eom.summary_oncology_member_ind,
    facility_dimension_eom.summary_cath_lab_member_ind,
    facility_dimension_eom.summary_intl_member_ind,
    facility_dimension_eom.summary_other_member_ind,
    facility_dimension_eom.pas_coid,
    facility_dimension_eom.pas_status,
    facility_dimension_eom.company_code_operations,
    facility_dimension_eom.osg_pas_ind,
    facility_dimension_eom.abs_facility_member_ind,
    facility_dimension_eom.abl_facility_member_ind,
    facility_dimension_eom.intl_pmis_member_ind,
    facility_dimension_eom.hsc_member_ind,
    facility_dimension_eom.source_system_code,
    facility_dimension_eom.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.facility_dimension_eom
;
