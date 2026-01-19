##########################
## Variable Declaration ##
##########################

BEGIN
DECLARE
tolerance_percent,difference,srctableid,src_rec_count,tgt_rec_count int64;
declare
sourcesysnm,srctablename,tgttablename,audit_type,tableload_run_time,job_name,audit_status string;
declare
tableload_start_time,tableload_end_time,audit_time,current_ts datetime;
SET
current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
SET 
srctableid = Null;
SET
sourcesysnm = @p_source;
SET
srctablename = Null;
SET
tgttablename = concat('edwhr.',@p_table);
SET
audit_type ='VALIDATION_COUNT';
SET
tableload_start_time = @p_tableload_start_time;
SET
tableload_end_time = @p_tableload_end_time;
SET
tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET
job_name = @p_job_name;
SET
audit_time = current_ts;
SET
tolerance_percent = 5;
SET
src_rec_count = 
(select count(*)
from 
(
select * from 

        {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_6_inc
      WHERE (
	   concat(coalesce(trim(cast(employee_action_detail_wrk_6_inc.employee_num as string)), '')	   
	  ,employee_action_detail_wrk_6_inc.employee_action_sid
	  ,trim(employee_action_detail_wrk_6_inc.action_code )
	  ,employee_action_detail_wrk_6_inc.employee_sid 
	  ,employee_action_detail_wrk_6_inc.action_sequence_num
	  ,employee_action_detail_wrk_6_inc.action_from_date 
	  ,coalesce(cast(trim(employee_action_detail_wrk_6_inc.from_auxiliary_status_code) as STRING), '')
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_dept_code as string)), '')
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_employee_pay_rate_amount as string)), '')	  
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_employee_work_schedule_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_job_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_location_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_overtime_plan_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_position_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_position_level_num as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_process_level_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_payment_frequency as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_payment_grade_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_payment_step_num as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_salary_class_code as string)), '')
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_schedule_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_standard_hour as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_supervisor_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_union_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_user_level as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_excemption_flag as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_expense_account_unit as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_expense_company as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_expense_sub_account_num as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.hr_code_desc as string)), '') 
	  ,employee_action_detail_wrk_6_inc.last_update_date
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.last_transfer_eff_date as string)), '')
	  ,coalesce(employee_action_detail_wrk_6_inc.reason_desc, '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_payment_frequency as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_auxiliary_status_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_dept_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_employee_pay_rate_amt as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_employee_schedule_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_job_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_location_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_overtime_plan_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_position_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_position_level_num as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_process_level_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_pay_grade_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_pay_step_num as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_salary_class_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_schedule_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_standard_hour as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_supervisor_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_union_code as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_user_level as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_exemption_flag as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_expense_account_unit as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_expense_sub_account_num as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.transfer_termination_flag as string)), '') 
	  ,coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_expense_company_num as string)), '') 
	  ,employee_action_detail_wrk_6_inc.lawson_company_num 
	  ,employee_action_detail_wrk_6_inc.process_level_code 
	  ,employee_action_detail_wrk_6_inc.source_system_code)) 
	  
	  NOT IN(
        SELECT  
		concat(coalesce(trim(cast(employee_action_detail.employee_num as STRING )),'')		
             ,employee_action_detail.employee_action_sid
             ,trim(employee_action_detail.action_code)
             ,employee_action_detail.employee_sid
             ,employee_action_detail.action_sequence_num
             ,employee_action_detail.action_from_date
              ,coalesce(trim(cast(employee_action_detail.from_auxiliary_status_code as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_dept_code as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_employee_pay_rate_amount as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_employee_work_schedule_code as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_job_code as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_location_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_overtime_plan_code as STRING)), '')
            ,coalesce(trim(cast(employee_action_detail.from_position_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_position_level_num as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_process_level_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_payment_frequency as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_payment_grade_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_payment_step_num as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_salary_class_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_schedule_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_standard_hour as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_supervisor_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_union_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_user_level  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_excemption_flag  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_expense_account_unit  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_expense_company as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.from_expense_sub_account_num as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.hr_code_desc  as STRING )),'')
            ,employee_action_detail.last_update_date
		   ,coalesce(trim(cast(employee_action_detail.last_transfer_eff_date as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.reason_desc  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_payment_frequency as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_auxiliary_status_code as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_dept_code  as STRING) ),'')
            ,coalesce(trim(cast(employee_action_detail.to_employee_pay_rate_amt as STRING) ),'')
            ,coalesce(trim(cast(employee_action_detail.to_employee_schedule_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_job_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_location_code as STRING)), '')
            ,coalesce(trim(cast(employee_action_detail.to_overtime_plan_code  as STRING) ),'')
            ,coalesce(trim(cast(employee_action_detail.to_position_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_position_level_num as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_process_level_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_pay_grade_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_pay_step_num  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_salary_class_code  as STRING) ),'')
            ,coalesce(trim(cast(employee_action_detail.to_schedule_code  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_standard_hour  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_supervisor_code  as STRING) ),'')
            ,coalesce(trim(cast(employee_action_detail.to_union_code  as STRING)),'')
            ,coalesce(trim(cast(employee_action_detail.to_user_level  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_exemption_flag  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_expense_account_unit  as STRING )),'')
            ,coalesce(trim(cast(employee_action_detail.to_expense_sub_account_num as STRING) ),'')
            ,coalesce(trim(cast(employee_action_detail.transfer_termination_flag  as STRING) ),'')
            ,coalesce(trim(cast(employee_action_detail.to_expense_company_num as STRING) ),'')
             ,employee_action_detail.lawson_company_num
             ,employee_action_detail.process_level_code
             ,employee_action_detail.source_system_code)
          FROM
            {{ params.param_hr_base_views_dataset_name }}.employee_action_detail for system_time as of timestamp(tableload_start_time,'US/Central')
          WHERE DATE(employee_action_detail.valid_to_date) = DATE("9999-12-31")
          and dw_last_update_date_time < tableload_start_time  - interval 1 minute)
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.employee_action_detail
where   date(valid_to_date) = '9999-12-31'
and dw_last_update_date_time >= tableload_start_time  - interval 1 minute) ;

SET
difference = CASE 
              WHEN src_rec_count <> 0 Then CAST(((ABS(tgt_rec_count - src_rec_count)/src_rec_count) * 100) AS INT64)
              WHEN src_rec_count =0 and tgt_rec_count = 0 Then 0
              ELSE tgt_rec_count
              END;

SET
audit_status = CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
END;

##Insert statement
INSERT INTO
 {{ params.param_hr_audit_dataset_name }}.audit_control
VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type, src_rec_count, tgt_rec_count, 
  cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time,
   job_name, audit_time, audit_status );
END; 




