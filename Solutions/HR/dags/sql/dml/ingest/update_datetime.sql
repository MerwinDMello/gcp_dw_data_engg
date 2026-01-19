BEGIN 
DECLARE tab STRING;
DECLARE sql_query STRING;
DECLARE timezone STRING;
SET tab = @target;
SET timezone = 'US/Central';
SET sql_query = (SELECT FORMAT( '''
              update `{{params.param_hr_stage_dataset_name}}.%s`
set dw_last_update_date_time = datetime_trunc(current_datetime('%s'), SECOND)
where dw_last_update_date_time is null;''',tab,timezone ));
       
EXECUTE IMMEDIATE sql_query;

END;
