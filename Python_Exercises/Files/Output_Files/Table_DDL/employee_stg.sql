CREATE TABLE IF NOT EXISTS {{ params.param_stage_dataset_name }}.employee_stg
(
	employee_number int64,
	first_name string,
	last_name string,
	standard_hours float64,
	active int64,
	dw_last_update_date timestamp,
);