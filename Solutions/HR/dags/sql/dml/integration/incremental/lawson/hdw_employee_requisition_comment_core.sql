BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
  
BEGIN TRANSACTION;
insert into {{ params.param_hr_core_dataset_name }}.employee_requisition_comment
(
 	employee_sid
,	requisition_sid
,	applicant_type_id
,	comment_type_code
,	action_code
,	comment_line_num
,	sequence_num
,	hr_company_sid
,valid_from_date
,valid_to_date
,	lawson_company_num
,	comment_text
,	comment_date
,	print_code
,	process_level_code
,	requisition_num
,	employee_num
,	delete_ind
,	source_system_code
,	dw_last_update_date_time
)
select 
	stg.employee_sid
,	stg.requisition_sid
,	stg.applicant_type_id
,	stg.comment_type_code
,	stg.action_code
,	stg.comment_line_num
,	stg.sequence_num
,	stg.hr_company_sid
,current_ts AS valid_from_date
,DATETIME("9999-12-31 23:59:59") AS valid_to_date
,	stg.lawson_company_num
,	stg.comment_text
,	stg.comment_date
,	stg.print_code
,	stg.process_level_code
,	stg.requisition_num
,	stg.employee_num
,	stg.delete_ind
,	stg.source_system_code
,	current_datetime('US/Central')dw_last_update_date_time
from {{ params.param_hr_stage_dataset_name }}.employee_requisition_comment_wrk stg
left join {{ params.param_hr_core_dataset_name }}.employee_requisition_comment tgt
on 
	tgt.employee_sid = stg.employee_sid
	and tgt.requisition_sid = stg.requisition_sid
	and tgt.applicant_type_id = stg.applicant_type_id
	and upper(trim(tgt.comment_type_code)) = upper(trim(stg.comment_type_code))
	and upper(trim(tgt.action_code)) = upper(trim(stg.action_code))
	and tgt.comment_line_num = stg.comment_line_num
	and tgt.sequence_num = stg.sequence_num
	and tgt.hr_company_sid=stg.hr_company_sid
	and tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
where tgt.employee_sid is null
;



/*  update  delete_ind */


update {{ params.param_hr_core_dataset_name }}.employee_requisition_comment empc
set delete_ind = 'D'  
where  empc.delete_ind  = 'A'
and    (empc.lawson_company_num||'-'||empc.employee_num) not in 
(select distinct lawson_company_num||'-'||employee_num from {{ params.param_hr_core_dataset_name }}.employee)
AND UPPER(empc.source_system_code) = 'L';
--and (empc.employee_num is not null or empc.employee_num not= 0);



update {{ params.param_hr_core_dataset_name }}.employee_requisition_comment empc
set delete_ind = 'A'
where  empc.delete_ind  = 'D'
and     (empc.lawson_company_num||'-'|| empc.employee_num) in 
(select distinct lawson_company_num||'-'|| employee_num from {{ params.param_hr_core_dataset_name }}.employee)
AND UPPER(empc.source_system_code) = 'L';



  
  
/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select employee_sid ,requisition_sid ,applicant_type_id ,comment_type_code ,action_code ,comment_line_num ,sequence_num ,hr_company_sid ,valid_from_date
        from {{ params.param_hr_core_dataset_name }}.employee_requisition_comment
        group by employee_sid ,requisition_sid ,applicant_type_id ,comment_type_code ,action_code ,comment_line_num ,sequence_num ,hr_company_sid ,valid_from_date		
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: employee_requisition_comment');
    ELSE
      COMMIT TRANSACTION;
    END IF;



end;