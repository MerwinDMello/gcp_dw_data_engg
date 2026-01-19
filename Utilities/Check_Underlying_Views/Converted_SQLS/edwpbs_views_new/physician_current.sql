-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/physician_current.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.physician_current AS SELECT
    physician_current.hcp_dw_id,
    physician_current.gl_provider_dept_num,
    physician_current.coid,
    physician_current.company_code,
    physician_current.ancillary_dept_num,
    physician_current.assigned_start_date,
    physician_current.budget_coid,
    physician_current.budget_oh_dept_num,
    physician_current.budget_start_date,
    physician_current.coid_dept_name,
    physician_current.coid_dept_start_date,
    physician_current.coid_dept_term_date,
    physician_current.coid_start_date,
    physician_current.community_status,
    physician_current.compensation_type,
    physician_current.src_sys_compensation_type_key,
    physician_current.contract_expiration_date,
    physician_current.contract_renewal_date,
    physician_current.development_type_name,
    physician_current.src_sys_development_key,
    physician_current.birth_date,
    physician_current.effective_date,
    physician_current.provider_first_name,
    physician_current.fte,
    physician_current.is_budget_ind,
    physician_current.is_hospitalist_ind,
    physician_current.is_multiple_provider_dept_ind,
    physician_current.provider_last_name,
    physician_current.provider_middle_initial,
    physician_current.new_replaced_ind,
    physician_current.new_replaced_name,
    physician_current.overhead_allocation,
    physician_current.original_start_date,
    physician_current.npi,
    physician_current.practice_address_1,
    physician_current.practice_address_2,
    physician_current.practice_city,
    physician_current.practice_fax,
    physician_current.practice_manager_first_name,
    physician_current.practice_manager_last_name,
    physician_current.practice_name,
    physician_current.src_sys_practice_num,
    physician_current.practice_phone,
    physician_current.practice_state,
    physician_current.practice_zip,
    physician_current.projected_termination_date,
    physician_current.provider_head_count,
    physician_current.src_sys_provider_key,
    physician_current.relationship_ind,
    CASE
      WHEN session_user() = zz.userid THEN substr(CAST(/* expression of unknown or erroneous type */ physician_current.social_security_num as STRING), length(CAST(/* expression of unknown or erroneous type */ physician_current.social_security_num as STRING)) - 9, 9)
      ELSE '***'
    END AS social_security_num,
    physician_current.specialty_code,
    physician_current.specialty_name,
    physician_current.specialty_type,
    physician_current.termination_reason_name,
    physician_current.termination_type_name,
    physician_current.termination_type_id,
    physician_current.termination_results,
    physician_current.upin,
    physician_current.status,
    physician_current.pe_date,
    physician_current.data_source_code,
    physician_current.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.physician_current
    LEFT OUTER JOIN (
      SELECT
          sme.userid,
          sme.masked_column_code
        FROM
          `hca-hin-dev-cur-parallon`.edw_sec.security_mask_and_exception AS sme
        WHERE session_user() = sme.userid
         AND upper(sme.masked_column_code) = 'SSN'
    ) AS zz ON session_user() = zz.userid
;
