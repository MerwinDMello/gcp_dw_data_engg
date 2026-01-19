-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/facility_dimension_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.facility_dimension_eom
   OPTIONS(description='Facility Dimension End of Month Snapshot')
  AS SELECT
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
      {{ params.param_pbs_core_dataset_name }}.facility_dimension_eom
  ;
