create table if not exists `{{ params.param_hr_stage_dataset_name }}.person_action_wrk5`
(
  person_action_sid INT64 NOT NULL,
  effect_date DATE NOT NULL
)
