Create or Replace view {{ params.param_parallon_ra_views_dataset_name }}.mon_account_payer_calc_summary
as
SELECT rcs.coid as coid,cs.* FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_summary cs
inner join {{ params.param_parallon_ra_stage_dataset_name }}.org org 
          on cs.org_id=org.org_id
inner join {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure rcs 
          on org.org_id = rcs.org_id
          and org.schema_id = rcs.schema_id
inner join {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility as b 
          on upper(rtrim(rcs.coid)) = upper(rtrim(b.co_id)) and rtrim(b.user_id) = rtrim(session_user());