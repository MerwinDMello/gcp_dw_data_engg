BEGIN

create temp table t3 as
select * from (
select * from (SELECT *, ROW_NUMBER() OVER (PARTITION BY
hca_unique,surv_id ,adj_samp,survey_type,pg_unit ,disdate ,recdate ,mde ,question_id ,coid ,response ,ptype ,qtype ,category ,cms_rpt ORDER BY 
hca_unique,surv_id ,adj_samp,survey_type,pg_unit ,disdate ,recdate ,mde ,question_id ,coid ,response ,ptype ,qtype ,category ,cms_rpt ) ROW 
FROM {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_all) t  where t.ROW=1 ) t2 ;

truncate table {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_all ;

insert into {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_all
select  
hca_unique,surv_id ,adj_samp,survey_type,pg_unit ,disdate ,recdate ,mde ,question_id ,coid ,response ,ptype ,qtype ,category ,cms_rpt,timestamp_trunc(current_datetime('US/Central'), SECOND)
from t3  ;

END;