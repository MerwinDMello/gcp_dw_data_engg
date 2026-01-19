Create or Replace view {{ params.param_parallon_ra_views_dataset_name }}.org
as 
SELECT * FROM {{ params.param_parallon_ra_stage_dataset_name }}.org;
