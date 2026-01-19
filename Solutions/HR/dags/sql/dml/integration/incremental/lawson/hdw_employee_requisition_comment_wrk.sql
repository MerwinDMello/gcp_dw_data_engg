BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET
	current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_requisition_comment_wrk;

insert into
	{{ params.param_hr_stage_dataset_name }}.employee_requisition_comment_wrk (
		employee_sid,
		requisition_sid,
		applicant_type_id,
		comment_type_code,
		action_code,
		comment_line_num,
		sequence_num,
		hr_company_sid,
		valid_from_date,
		valid_to_date,
		lawson_company_num,
		comment_text,
		comment_date,
		print_code,
		process_level_code,
		requisition_num,
		employee_num,
		delete_ind,
		source_system_code,
		dw_last_update_date_time
	)
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
	);

BEGIN TRANSACTION;

update
	{{ params.param_hr_core_dataset_name }}.employee_requisition_comment as tgt
set
	valid_to_date = current_ts - INTERVAL 1 SECOND,
	dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND)
from
	{{ params.param_hr_stage_dataset_name }}.employee_requisition_comment_wrk as stg
where
	tgt.employee_sid = stg.employee_sid
	and tgt.requisition_sid = stg.requisition_sid
	and tgt.applicant_type_id = stg.applicant_type_id
	and tgt.comment_type_code = stg.comment_type_code
	and tgt.action_code = stg.action_code
	and tgt.comment_line_num = stg.comment_line_num
	and tgt.sequence_num = stg.sequence_num
	and tgt.hr_company_sid = stg.hr_company_sid
	and tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
	and (
		stg.lawson_company_num <> tgt.lawson_company_num
		or coalesce(upper(trim(stg.comment_text)), '~') <> coalesce(upper(trim(tgt.comment_text)), '~')
		or coalesce(stg.comment_date, date '9999-12-01') <> coalesce(tgt.comment_date, date '9999-12-01')
		or coalesce(upper(trim(stg.print_code)), '~') <> coalesce(upper(trim(tgt.print_code)), '~')
		or stg.process_level_code <> tgt.process_level_code
		or stg.requisition_num <> tgt.requisition_num
		or stg.employee_num <> tgt.employee_num
	)
	AND UPPER(tgt.source_system_code) = 'L';

-- this retires the records which are not received from the source but present in the core table with valid_to_date='9999-12-31'
update
	{{ params.param_hr_core_dataset_name }}.employee_requisition_comment
set
	valid_to_date = current_ts - interval '1' SECOND,
	dw_last_update_date_time = current_datetime('US/Central')
where
	(
		employee_sid || requisition_sid || applicant_type_id || upper(trim(comment_type_code)) || upper(trim(action_code)) || comment_line_num || sequence_num || hr_company_sid
	) not in (
		select
			employee_sid || requisition_sid || applicant_type_id || upper(trim(comment_type_code)) || upper(trim(action_code)) || comment_line_num || sequence_num || hr_company_sid
		from
			{{ params.param_hr_stage_dataset_name }}.employee_requisition_comment_wrk
	)
	and valid_to_date = DATETIME("9999-12-31 23:59:59")
	AND UPPER(employee_requisition_comment.source_system_code) = 'L';

/* Test Unique Primary Index constraint set in Terdata */
SET
	DUP_COUNT = (
		select
			count(*)
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
					valid_from_date
				from
					{{ params.param_hr_core_dataset_name }}.employee_requisition_comment
				group by
					employee_sid,
					requisition_sid,
					applicant_type_id,
					comment_type_code,
					action_code,
					comment_line_num,
					sequence_num,
					hr_company_sid,
					valid_from_date
				having
					count(*) > 1
			)
	);

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat(
	'Duplicates are not allowed in the table: employee_requisition_comment'
);

ELSE COMMIT TRANSACTION;

END IF;

END;