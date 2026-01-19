create table if not exists {{ params.param_hr_core_dataset_name }}.survey_question (
survey_question_sid int64 not null
, eff_from_date date not null
, survey_sid int64 not null
, survey_sub_category_text string
, base_question_id string
, question_id string not null
, question_type_code string not null
, question_short_name string
, question_desc string
, question_seq_num int64 not null
, top_box_num int64
, top_box_high_num int64
, measure_id_text string
, legacy_question_id int64
, standard_flag string
, ignore_value int64
, eff_to_date date not null
, source_system_code string not null
, dw_last_update_date_time datetime not null
)
partition by eff_from_date
cluster by survey_question_sid, eff_from_date
;


