BEGIN 
DECLARE tab STRING;
DECLARE sql_query STRING;
SET tab = @target;
SET sql_query = (SELECT FORMAT( '''
              update `{{params.param_hr_stage_dataset_name}}.%s`
set dw_last_update_date_time = timestamp_trunc(current_timestamp(), SECOND), source_system_code = 'H'
where dw_last_update_date_time is null;''',tab ));
       
EXECUTE IMMEDIATE sql_query;

END;