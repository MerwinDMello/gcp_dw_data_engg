BEGIN

create temp table t3 as
select * from (
select * from (SELECT *, ROW_NUMBER() OVER (PARTITION BY
question_id, response, response_label ORDER BY 
question_id, response, response_label ) ROW 
FROM {{ params.param_hr_stage_dataset_name }}.hdw_resp_lbl) t  where t.ROW=1 ) t2 ;

truncate table {{ params.param_hr_stage_dataset_name }}.hdw_resp_lbl ;

insert into {{ params.param_hr_stage_dataset_name }}.hdw_resp_lbl
select  
question_id, response, response_label,timestamp_trunc(current_datetime("US/Central"), SECOND)
from t3  ;

END;