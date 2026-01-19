create or replace view `{{ params.param_hr_base_views_dataset_name }}.supervisor`
AS SELECT
    supervisor.supervisor_sid,
    supervisor.valid_from_date,
    supervisor.valid_to_date,
    supervisor.eff_from_date,
    supervisor.employee_sid,
    supervisor.employee_num,
    supervisor.hr_company_sid,
    supervisor.reporting_supervisor_sid,
    supervisor.supervisor_code,
    supervisor.supervisor_desc,
    supervisor.officer_code,
    supervisor.lawson_company_num,
    supervisor.process_level_code,
    supervisor.active_dw_ind,
    supervisor.security_key_text,
    supervisor.source_system_code,
    supervisor.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.supervisor;