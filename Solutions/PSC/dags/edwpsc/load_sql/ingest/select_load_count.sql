SELECT COUNT(*) as Rec_Count 
FROM {{ params.param_psc_stage_dataset_name }}.{{ params.param_load_table_name }};