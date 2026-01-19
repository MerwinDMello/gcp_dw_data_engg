Create or Replace view {{ params.param_parallon_ra_views_dataset_name }}.credit_refund_transmitted
as 
SELECT dro.coid as coid,a.* FROM {{ params.param_parallon_ra_stage_dataset_name }}.cr_refund_transmitted AS a
inner join {{ params.param_parallon_pbs_base_views_dataset_name }}.dim_rcm_organization dro
            on a.unit_num=dro.unit_num
inner join {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility as b 
            on upper(rtrim(dro.coid)) = upper(rtrim(b.co_id)) and rtrim(b.user_id) = rtrim(session_user());
			