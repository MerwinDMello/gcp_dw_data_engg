Select
COUNT(*) as Rec_Count
From {{ params.param_im_stage_dataset_name }}.{{ params.param_im_object_name }};