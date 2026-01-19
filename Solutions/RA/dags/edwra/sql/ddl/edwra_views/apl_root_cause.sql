Create or Replace view {{ params.param_parallon_ra_views_dataset_name }}.apl_root_cause
as 
SELECT * FROM {{ params.param_parallon_ra_stage_dataset_name }}.apl_root_cause;
