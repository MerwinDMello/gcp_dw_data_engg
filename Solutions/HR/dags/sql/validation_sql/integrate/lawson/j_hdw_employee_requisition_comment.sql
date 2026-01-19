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
select
	employee_sid,
	requisition_sid,
	applicant_type_id,
	comment_type_code,
	action_code,
	comment_line_num,
	sequence_num,
	hr_company_sid,
	current_datetime('US/Central') as valid_from_date,
	DATETIME("9999-12-31 23:59:59") as valid_to_date,
	lawson_company_num,
	comment_text,
	comment_date,
	print_code,
	'00000' as process_level_code,
	requisition_num,
	employee_num,
	'A' as delete_ind,
	source_system_code,
	current_datetime('US/Central') as dw_last_update_date_time
from
	(
		select
			case
				when stg.employee = 0 then 0
				else emp.employee_sid
			end as employee_sid,
			case
				when coalesce(cast(nullif(trim(stg.job_code),'') as integer),0) = 0 then 0
				else req.requisition_sid
			end as requisition_sid,
			stg.emp_app as applicant_type_id,
			stg.cmt_type as comment_type_code,
			stg.action_code as action_code,
			stg.ln_nbr as comment_line_num,
			stg.seq_nbr as sequence_num,
			hrc.hr_company_sid as hr_company_sid,
			stg.company as lawson_company_num,
			stg.cmt_text as comment_text,
			stg.r_date as comment_date,
			stg.print_code as print_code,
			coalesce(cast(nullif(trim(stg.job_code),'') as integer),0) as requisition_num,
			stg.employee as employee_num,
			'L' as source_system_code
		from
			{{ params.param_hr_stage_dataset_name }}.pacomments_stg stg
			inner join {{ params.param_hr_base_views_dataset_name }}.employee emp on stg.employee = emp.employee_num
			and stg.company = emp.lawson_company_num
			and emp.valid_to_date = DATETIME("9999-12-31 23:59:59")
			left join {{ params.param_hr_base_views_dataset_name }}.requisition req on cast(nullif(trim(stg.job_code),'') as integer) = req.requisition_num
			and stg.company = req.lawson_company_num
			and req.valid_to_date = DATETIME("9999-12-31 23:59:59")
			inner join {{ params.param_hr_base_views_dataset_name }}.hr_company hrc on stg.company = hrc.lawson_company_num
			and hrc.valid_to_date = DATETIME("9999-12-31 23:59:59")
		where
			stg.emp_app <> 1 qualify row_number() over(
				partition by employee_sid,
				requisition_sid,
				applicant_type_id,
				comment_type_code,
				action_code,
				comment_line_num,
				sequence_num,
				hr_company_sid,
				lawson_company_num,
				comment_text,
				comment_date,
				print_code,
				requisition_num,
				employee_num
				order by
					employee_sid
			) = 1
	)
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.employee_requisition_comment
where date(valid_to_date) = '9999-12-31'  AND source_system_code = 'L'
) ;

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




