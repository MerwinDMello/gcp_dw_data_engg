BEGIN
DECLARE current_dt DATETIME;
SET current_dt = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);


call
  {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}',
  'taleo_jobtemplate','job_template_num', 'JOB_TEMPLATE');

--call edwhr_procs.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_jobtemplate', 'trim(job_template_num)', 'job_template');

  truncate table {{ params.param_hr_stage_dataset_name }}.job_template_wrk;

/*  load work table with working data */
BEGIN TRANSACTION;


  insert into {{ params.param_hr_stage_dataset_name }}.job_template_wrk (file_date, job_template_sid, valid_from_date, job_template_num, base_job_template_num, recruitment_job_sid, job_template_status_id, valid_to_date, source_system_code, dw_last_update_date_time)
    select
        stg.file_date,
        cast(xwlk.sk as int64) as job_template_sid,
        stg.file_date as valid_from_date,
        case
           trim(stg.job_template_num)
          when '' then 0
          else cast(trim(stg.job_template_num) as int64)
        end as job_template_num,
        case
           trim(stg.basejobtemplate_number)
          when '' then 0
          else cast(trim(stg.basejobtemplate_number) as int64)
        end as base_job_template_num,
        rj.recruitment_job_sid,
        case
           trim(stg.state_number)
          when '' then 0
          else cast(trim(stg.state_number) as int64)
        end as job_template_status_id,
        DATETIME("9999-12-31 23:59:59+00") as valid_to_date,
        'T' as source_system_code,
        timestamp_trunc(current_datetime(), SECOND) as dw_last_update_date_time
        --timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      from
        {{ params.param_hr_stage_dataset_name }}.taleo_jobtemplate as stg
        inner join {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk as xwlk on upper(substr(trim(stg.job_template_num), 1, 255)) = upper(xwlk.sk_source_txt)
         and upper(xwlk.sk_type) = 'job_template'
        left outer join -- inner join {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk xwlk_rj
        -- on (cast( trim(stg.job_template_num)   as varchar(255)) = xwlk_rj.sk_source_txt and xwlk_rj.sk_type = 'recruitment_job')
        {{ params.param_hr_base_views_dataset_name }}.recruitment_job as rj on upper(substr(trim(stg.jobinformation_number), 1, 255)) = upper(cast(rj.recruitment_job_num as string))
      group by 1, 2, 1, 4, 5, 6, 7, 8, 9, 10
  ;
END