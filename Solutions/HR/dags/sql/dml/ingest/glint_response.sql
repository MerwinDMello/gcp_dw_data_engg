BEGIN

DECLARE question_columns STRING;
DECLARE comment_columns STRING;

SET question_columns = (
  select concat('(', string_agg(distinct column_name), ')'),
  from(select lower(column_name) as column_name from {{ params.param_hr_stage_dataset_name }}.INFORMATION_SCHEMA.COLUMNS
  where table_name ="glint_engagement_external_table" 
  and lower(column_name) in ( select lower(question_label) from {{ params.param_hr_stage_dataset_name }}.glint_question ))
);

SET comment_columns = (
  select concat('(', string_agg(distinct column_name), ')'),
  from(select lower(column_name) as column_name from {{ params.param_hr_stage_dataset_name }}.INFORMATION_SCHEMA.COLUMNS
  where table_name ="glint_engagement_external_table" 
  and lower(column_name) in ( select concat(lower(question_label),'_comment') from {{ params.param_hr_stage_dataset_name }}.glint_question ))
);

truncate table {{ params.param_hr_stage_dataset_name }}.glint_response;

EXECUTE IMMEDIATE format("""
  insert into {{ params.param_hr_stage_dataset_name }}.glint_response
  select 
    NULLIF(TRIM(cast(response.survey_creation_date as string)),''),
    NULLIF(TRIM(cast(response.survey_completion_date as string)),''),
    NULLIF(TRIM(cast(response.survey_sent_date as string)),''), 
    NULLIF(TRIM(response.survey_cycle_uuid),''),
    NULLIF(TRIM(response.survey_cycle_title),''), 
    NULLIF(TRIM(response.employee_id),''),
    NULLIF(TRIM(response.first_name),''),
    NULLIF(TRIM(response.last_name),''),
    NULLIF(TRIM(response.email),''),
    NULLIF(TRIM(response.question),''),
    NULLIF(TRIM(cast(response.response as string)),''),
    NULLIF(TRIM(REGEXP_REPLACE(TRIM(comments), r'([^\\p{ASCII}]+)', '')),''),
    timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
  from(
  (select
    survey_creation_date, 
    survey_completion_date, 
    survey_sent_date, 
    survey_cycle_uuid, 
    survey_cycle_title, 
    employee_id,
    first_name, 
    last_name, 
    email,
    response,
    question,
    from {{ params.param_hr_stage_dataset_name }}.glint_engagement_external_table
    unpivot include nulls(response FOR question IN %s)) as response
  inner join  
  (select
    survey_creation_date, 
    survey_completion_date, 
    survey_sent_date, 
    survey_cycle_uuid, 
    survey_cycle_title, 
    employee_id,
    first_name, 
    last_name, 
    email,
    comments,
    question 
    from {{ params.param_hr_stage_dataset_name }}.glint_engagement_external_table
    unpivot include nulls(comments FOR question IN %s)) as comment
    on response.email = comment.email
    and comment.question = concat(response.question, '_comment'))
""", question_columns, comment_columns
);

END;