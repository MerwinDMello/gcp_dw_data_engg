BEGIN

DECLARE tab STRING;
DECLARE sql_query STRING;
SET tab = @target;


SET sql_query = (SELECT FORMAT( '''
begin

DROP TABLE IF EXISTS t3;

create temp table t3 as
select * from (
select * from (SELECT *, ROW_NUMBER() OVER (PARTITION BY
hca_unique,surv_id ,adj_samp,survey_type,pg_unit ,disdate ,recdate ,mde ,question_id ,coid ,response ,ptype ,qtype ,category ,cms_rpt ORDER BY 
hca_unique,surv_id ,adj_samp,survey_type,pg_unit ,disdate ,recdate ,mde ,question_id ,coid ,response ,ptype ,qtype ,category ,cms_rpt ) ROW 
FROM {{ params.param_hr_stage_dataset_name }}.tab) t  where t.ROW=1 ) t2 ;

truncate table {{ params.param_hr_stage_dataset_name }}.tab ;

insert into {{ params.param_hr_stage_dataset_name }}.tab
select  
hca_unique,surv_id ,adj_samp,survey_type,pg_unit ,disdate ,recdate ,mde ,question_id ,coid ,response ,ptype ,qtype ,category ,cms_rpt,timestamp_trunc(current_timestamp(), SECOND)
from t3  ;
end;''',tab ));
     
EXECUTE IMMEDIATE sql_query;



/*
SET sql_query = (SELECT FORMAT( '''
              update `{{params.param_hr_stage_dataset_name}}.%s`
set dw_last_update_date_time = timestamp_trunc(current_timestamp(), SECOND)
where dw_last_update_date_time is null;''',tab ));
     
EXECUTE IMMEDIATE sql_query;*/

END;