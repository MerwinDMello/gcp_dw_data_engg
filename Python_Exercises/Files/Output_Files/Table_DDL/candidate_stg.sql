CREATE TABLE IF NOT EXISTS {{ params.param_stage_dataset_name }}.candidate_stg
(
	candidate_number int64,
	first_name string,
	last_name string,
	birth_date string,
	do_not_hire int64,
	dw_last_update_date timestamp,
);