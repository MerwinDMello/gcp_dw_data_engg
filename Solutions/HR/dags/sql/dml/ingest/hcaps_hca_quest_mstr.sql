BEGIN

create temp table t3 as
select * from (
select * from (SELECT *, ROW_NUMBER() OVER (PARTITION BY
qtype, ptype, category, sub_category, qshort_txt, qtxt, question_id, survey_ord, tboxvalue, tbox_high_val, std_flag, ignore_val ORDER BY 
qtype, ptype, category, sub_category, qshort_txt, qtxt, question_id, survey_ord, tboxvalue, tbox_high_val, std_flag, ignore_val ) ROW 
FROM {{ params.param_hr_stage_dataset_name }}.hca_quest_mstr) t  where t.ROW=1 ) t2 ;

truncate table {{ params.param_hr_stage_dataset_name }}.hca_quest_mstr ;

insert into {{ params.param_hr_stage_dataset_name }}.hca_quest_mstr
select  
qtype, ptype, category, sub_category, qshort_txt, qtxt, question_id, survey_ord, tboxvalue, tbox_high_val, std_flag, ignore_val,timestamp_trunc(current_datetime("US/Central"), SECOND)
from t3  ;

END;