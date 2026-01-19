-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/facility_dimension_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.facility_dimension_eom
   OPTIONS(description='Facility Dimension End of Month Snapshot')
  AS SELECT
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
      {{ params.param_pbs_base_views_dataset_name }}.facility_dimension_eom AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
