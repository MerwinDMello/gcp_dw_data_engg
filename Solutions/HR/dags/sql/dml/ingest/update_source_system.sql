BEGIN 
DECLARE tab STRING;
DECLARE sql_query STRING;
DECLARE timezone STRING;
SET tab = @target;
SET timezone = 'US/Central';
SET sql_query = (SELECT FORMAT( '''
              update `{{params.param_hr_stage_dataset_name}}.%s`
set source_system_code = '{{params.param_source_system_code}}'
where source_system_code is null;''',tab ));
       
EXECUTE IMMEDIATE sql_query;

END;
