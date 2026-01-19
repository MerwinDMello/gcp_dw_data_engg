Create or Replace view {{ params.param_parallon_ra_views_dataset_name }}.mon_account
as 
SELECT * FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account;
