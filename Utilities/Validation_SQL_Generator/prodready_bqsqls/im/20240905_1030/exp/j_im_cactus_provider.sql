-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_cactus_provider.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT t7.im_domain_id,
          cactus_provider_user_id AS cactus_provider_user_id,
          t1.hcp_src_sys_key,
          t1.hcp_npi,
          t6.fac_asgn_stts_sid,
          t6.fac_asgn_stts_src_sys_key,
          t2.prov_cat_sid,
          t2.prov_cat_src_sys_key,
          t1.hcp_first_name,
          t1.hcp_last_name,
          t1.hcp_middle_name,
          CASE
              WHEN rtrim(t6.fac_asgn_stts_desc) IN('Current',
                                                   'Temporary') THEN 1
              ELSE 0
          END AS cactus_provider_activity_exempt_sw,
          'K' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_im_base_views_dataset_name }}.hcp AS t1
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.ref_provider_category AS t2 ON t1.hcp_cat_sid = t2.prov_cat_sid
   AND upper(rtrim(t2.prov_cat_src_sys_key)) IN('D2G019AL2J',
                                                'D2G019AKNU')
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.hcp_other_id AS t3 ON t1.hcp_dw_id = t3.hcp_dw_id
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.ref_id_type AS t4 ON t3.id_type_sid = t4.id_type_sid
   AND upper(rtrim(t4.id_type_src_sys_key)) = 'D2QK0IXBT7'
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.hcp_facility_assignment AS t5 ON t1.hcp_dw_id = t5.hcp_dw_id
   AND upper(rtrim(t5.fac_asgn_active_ind)) = 'Y'
   INNER JOIN {{ params.param_im_base_views_dataset_name }}.ref_facility_asgn_status AS t6 ON t5.fac_asgn_stts_sid = t6.fac_asgn_stts_sid
   AND t6.entity_sid = 1
   INNER JOIN {{ params.param_im_stage_dataset_name }}.hpf_instance_facility_xwalk AS t7 ON upper(rtrim(t5.coid)) = upper(rtrim(t7.coid))
   AND upper(rtrim(t5.company_code)) = upper(rtrim(t7.company_code))
   CROSS JOIN UNNEST(ARRAY[ substr(t3.hcp_other_id, 1, 7) ]) AS cactus_provider_user_id
   WHERE upper(rtrim(t1.hcp_active_ind)) = 'Y'
     AND cactus_provider_user_id IS NOT NULL
     AND NOT trim(cactus_provider_user_id) = ''
     AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(cactus_provider_user_id, '[.]') = 0
     AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(cactus_provider_user_id), 4, 4), '[A-Za-z_]') = 0
     AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(cactus_provider_user_id), 1, 3), '[0-9_]') = 0 QUALIFY row_number() OVER (PARTITION BY t7.im_domain_id,
                                                                                                                                                                            upper(cactus_provider_user_id)
                                                                                                                                                               ORDER BY t5.hcp_fac_asgn_eff_to_date DESC) = 1 ) AS a