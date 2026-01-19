begin

declare  dup_count int64;
declare current_ts datetime;
set current_ts = current_datetime('US/Central');

create temp table pos_date as (
select distinct 
		ep.employee_sid, 
		ep.position_sid,
		coalesce(rkeyt1.key_talent_id,rkeyt2.key_talent_id,rkeyt3.key_talent_id,rkeyt4.key_talent_id,rkeyt5.key_talent_id,rkeyt6.key_talent_id,rkeyt7.key_talent_id,rkeyt8.key_talent_id) as key_talent_id,
    	coalesce(mat1.integrated_lob_id,mat4.integrated_lob_id,mat3.integrated_lob_id,mat2.integrated_lob_id) as integrated_lob_id, 
		ep.position_level_sequence_num,
		ep.eff_from_date, 
		ep.eff_to_date, 
		ep.fte_percent,
		ep.schedule_work_code,
		ep.working_location_code, 
		ep.job_code, 
		ep.employee_num,
		ep.dept_sid, 
		ep.account_unit_num, 
		ep.gl_company_num,
		ep.lawson_company_num, 
		ep.process_level_code,
		jp.job_code_sid,
		jc.job_class_sid,
		gl.coid,
		gl.company_code,
		sub_func.sub_functional_dept_num,
		sub_func.functional_dept_num,
		coalesce(s.aux_status_sid, jes.status_sid) as auxiliary_status_sid,
		es.emp_status_sid as employee_status_sid,
		pay.pay_rate_amt,
		d.date_id
from (select * 
		from {{ params.param_hr_base_views_dataset_name }}.employee_position
		where (eff_from_date >= '2016-01-01'
			  or eff_to_date >= '2016-01-01')
			  and valid_to_date = datetime("9999-12-31 23:59:59")
			  and lawson_company_num <> 300) ep

inner join (select distinct date_id 
			from {{ params.param_pub_views_dataset_name }}.lu_date 
			where date_id <= current_date('US/Central')) d
		on d.date_id between ep.eff_from_date and ep.eff_to_date

left outer join {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk gl
		on ep.gl_company_num = gl.gl_company_num
		and ep.account_unit_num = gl.account_unit_num
		and gl.valid_to_date = datetime("9999-12-31 23:59:59")

left outer join {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department sub_func
		on gl.dept_num = sub_func.dept_num
		and gl.coid = sub_func.coid
		and gl.company_code = sub_func.company_code

left join {{ params.param_hr_base_views_dataset_name }}.junc_employee_status jes
		on ep.employee_sid = jes.employee_sid
		and upper(trim(jes.status_type_code)) = 'AUX'
		and jes.valid_to_date = datetime("9999-12-31 23:59:59")

left outer join {{ params.param_hr_base_views_dataset_name }}.job_position jp
		on ep.position_sid = jp.position_sid
		and ep.eff_from_date between jp.eff_from_date and jp.eff_to_date
		and jp.valid_to_date = datetime("9999-12-31 23:59:59")

left outer join {{ params.param_hr_base_views_dataset_name }}.job_code jc
		on jp.job_code_sid = jc.job_code_sid
		and jc.valid_to_date = datetime("9999-12-31 23:59:59")

left outer join (
				select	company_pay_schedule_sid, 
						pay_grade_code, 
						pay_step_num,
						eff_from_date, 
						--modified by thomas jones 12/16
						case when min(eff_from_date) over (partition by company_pay_schedule_sid,pay_grade_code,lawson_company_num order by eff_from_date rows between 1 following and 1 following) is null then cast ('12/31/9999' as date format 'mm/dd/yyyy')
						else (min(eff_from_date) over (partition by company_pay_schedule_sid,pay_grade_code,lawson_company_num order by eff_from_date rows between 1 following and 1 following))-1 end as eff_to_date,
						pay_schedule_code,
						pay_rate_amt, 
						lawson_company_num
				from	{{ params.param_hr_base_views_dataset_name }}.company_pay_grade_schedule
				where valid_to_date=datetime("9999-12-31 23:59:59")
					and pay_step_num = 2
				) pay
		on jp.pay_grade_code = pay.pay_grade_code
		and jp.pay_grade_schedule_code = pay.pay_schedule_code
		and jp.lawson_company_num = pay.lawson_company_num
		--modified by thomas jones 12/16
		and ep.eff_from_date between pay.eff_from_date and pay.eff_to_date

left join {{ params.param_hr_stage_dataset_name }}.aux_status s
		on ep.employee_sid = s.employee_sid
		and d.date_id between s.status_from_date and s.status_to_date
		and upper(trim(s.aux_status_code)) in ('PRN','FT','PT','TEMP')

inner join {{ params.param_hr_stage_dataset_name }}.emp_status es
		on ep.employee_sid = es.employee_sid
		and d.date_id between es.status_from_date and es.status_to_date
		and upper(trim(es.emp_status_code)) in ('01','02','03','04','05')

left outer join (
				select distinct 
						ep.employee_sid, 
						ep.position_sid, 
						ep.employee_num,
						ep.position_level_sequence_num,
						ep.eff_to_date-ep.eff_from_date as pos_length,
						min(ep.eff_from_date) as start_date
				from {{ params.param_hr_base_views_dataset_name }}.employee_position ep
				where ep.valid_to_date = datetime("9999-12-31 23:59:59")
					and ep.lawson_company_num <> 300
				group by 1,2,3,4,5
				) min_eff
		on ep.employee_sid = min_eff.employee_sid
		and ep.position_sid = min_eff.position_sid
		and ep.eff_from_date = min_eff.start_date
		and ep.position_level_sequence_num = min_eff.position_level_sequence_num

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department dept   ---------------------------Thomas Add for ILOB-----------------------------------
ON ep.dept_sid = dept.dept_sid
AND date(dept.valid_to_date) = '9999-12-31'

LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility ff    ---------------------------Thomas Add for ILOB-----------------------------------
ON gl.coid = ff.coid
AND gl.company_code = ff.company_code

		------Intergrated LOB Mapping Start----------------------------------------------------------------------------------------------------------------------------------------------------


LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat1  --Process Level AND Dept Num
ON ep.process_level_code = mat1.process_level_code AND dept.dept_code = mat1.dept_code
AND mat1.match_level_num = 1

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat2  --LOB and Sub LOB
ON ff.lob_code = mat2.lob_code AND ff.sub_lob_code = mat2.sub_lob_code
AND mat2.match_level_num = 2

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat3  --Function and Sub Function
ON sub_func.functional_dept_desc = mat3.functional_dept_desc
AND sub_func.sub_functional_dept_desc = mat3.sub_functional_dept_desc
AND mat3.match_level_num = 3

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat4  --Process Level
ON ep.process_level_code = mat4.process_level_code
AND mat4.match_level_num = 4

--------KeyTalent Mapping Start-------------

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt1
ON rkeyt1.match_level_num = 1
AND jc.job_code=rkeyt1.job_code
AND jc.job_code_desc=rkeyt1.job_code_desc
AND UPPER(jp.position_code_desc) LIKE UPPER('ACMO%')

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt2
ON rkeyt2.match_level_num = 2
 AND jc.job_code=rkeyt2.job_code
 AND ff.lob_code=rkeyt2.lob_code
 AND jc.job_code_desc=rkeyt2.job_code_desc

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt3
ON rkeyt3.match_level_num = 3
AND jc.job_code=rkeyt3.job_code
AND jc.job_code_desc=rkeyt3.job_code_desc
AND jp.position_code_desc=rkeyt3.job_title_text

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt4
ON rkeyt4.match_level_num = 4
AND jc.job_code=rkeyt4.job_code
AND jp.position_code_desc=rkeyt4.job_title_text
AND ep.process_level_code=rkeyt4.process_level_code

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt5
ON rkeyt5.match_level_num = 5
AND jc.job_code=rkeyt5.job_code
AND ep.process_level_code=rkeyt5.process_level_code

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt6
ON rkeyt6.match_level_num = 6
AND jc.job_code=rkeyt6.job_code
AND ep.process_level_code=rkeyt6.process_level_code
AND jp.position_code_desc LIKE 'Dir Prgm%'

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt7
ON rkeyt6.match_level_num = 7
AND jc.job_code=rkeyt7.job_code
AND ep.process_level_code=rkeyt7.process_level_code
AND dept.dept_code BETWEEN '70000' AND '79999'

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt8
ON rkeyt8.match_level_num = 8
AND jc.job_code=rkeyt8.job_code
AND jc.job_code_desc=rkeyt8.job_code_desc

where ep.valid_to_date = datetime("9999-12-31 23:59:59")
	and ep.lawson_company_num <> 300
	and ep.eff_from_date = d.date_id
	and ep.eff_to_date >= '2016-01-01'

qualify row_number() over (partition by ep.employee_sid, ep.position_sid, ep.position_level_sequence_num, ep.eff_to_date order by ep.eff_from_date) = 1
);

---------------------------------------------------------------------------------------

create temp table step_2 as (
select 
		st.employee_sid as start_employee_sid, 
		st.position_sid as start_position_sid,
		st.key_talent_id as start_key_talent_id,
		st.integrated_lob_id as start_integrated_lob_id, 
		st.position_level_sequence_num as start_position_level_sequence_num,
		st.eff_from_date as start_eff_from_date, 
		st.eff_to_date as start_eff_to_date, 
		st.fte_percent as start_fte_percent,
		st.schedule_work_code as start_schedule_work_code,
		st.working_location_code as start_working_location_code, 
		st.job_code as start_job_code, 
		st.employee_num as start_employee_num,
		st.dept_sid as start_dept_sid, 
		st.account_unit_num as start_account_unit_num, 
		st.gl_company_num as start_gl_company_num,
		st.lawson_company_num as start_lawson_company_num, 
		st.process_level_code as start_process_level_code,
		st.job_code_sid as start_job_code_sid,
		st.job_class_sid as start_job_class_sid,
		st.coid as start_coid,
		st.company_code as start_company_code,
		st.sub_functional_dept_num as start_sub_functional_dept_num,
		st.functional_dept_num as start_functional_dept_num,
		st.auxiliary_status_sid as start_auxiliary_status_sid,
		st.employee_status_sid as start_employee_status_sid,
		st.pay_rate_amt as start_pay_rate_amt,
		st.date_id as start_date_id,
		ed.employee_sid as end_employee_sid, 
		ed.position_sid as end_position_sid, 
		ed.key_talent_id as end_key_talent_id,
		ed.integrated_lob_id as end_integrated_lob_id,
		ed.position_level_sequence_num as end_position_level_sequence_num,
		ed.eff_from_date as end_eff_from_date, 
		ed.eff_to_date as end_eff_to_date, 
		ed.fte_percent as end_fte_percent,
		ed.schedule_work_code as end_schedule_work_code,
		ed.working_location_code as end_working_location_code, 
		ed.job_code as end_job_code, 
		ed.employee_num as end_employee_num,
		ed.dept_sid as end_dept_sid, 
		ed.account_unit_num as end_account_unit_num, 
		ed.gl_company_num as end_gl_company_num,
		ed.lawson_company_num as end_lawson_company_num, 
		ed.process_level_code as end_process_level_code,
		ed.job_code_sid as end_job_code_sid,
		ed.job_class_sid as end_job_class_sid,
		ed.coid as end_coid,
		ed.company_code as end_company_code,
		ed.sub_functional_dept_num as end_sub_functional_dept_num,
		ed.functional_dept_num as end_functional_dept_num,
		ed.auxiliary_status_sid as end_auxiliary_status_sid,
		ed.employee_status_sid as end_employee_status_sid,
		ed.pay_rate_amt as end_pay_rate_amt,
		ed.date_id as end_date_id
from pos_date st

inner join pos_date ed
		on st.employee_sid = ed.employee_sid
		and st.position_level_sequence_num = ed.position_level_sequence_num
		and st.lawson_company_num = ed.lawson_company_num
		and st.eff_to_date = ed.date_id-1

where  st.process_level_code = ed.process_level_code
	--and st.position_sid <> ed.position_sid
);

---------------------------------------------------------------------------------------

create temp table pos_action as ( 
select distinct 
		employee_sid
		,action_code
		,action_reason_text
		,eff_from_date
		,lawson_company_num
from {{ params.param_hr_base_views_dataset_name }}.person_action
where valid_to_date = datetime("9999-12-31 23:59:59")
	and eff_from_date >= '2016-01-01'
	and lawson_company_num <> 300
	and upper(trim(action_code)) like '%XFER%'
	and action_sequence_num = 1 --solution to multiple 1xfer action on the same day and same postion and changed action_reason_text
qualify row_number() over (partition by employee_sid, eff_from_date, action_sequence_num order by valid_from_date desc) = 1
);

BEGIN TRANSACTION;
-- delete prior data for movement

delete
from {{ params.param_hr_core_dataset_name }}.fact_total_movement
where analytics_msr_sid in (
select analytics_msr_sid from  {{ params.param_dim_base_views_dataset_name }}.dim_analytics_measure
where upper(trim(analytics_msr_name_child)) ='HR_LAWSON_TRANSFERS')
and (to_lawson_company_num != 300
or from_lawson_company_num != 300);	

-- insert movement data

insert into  {{ params.param_hr_core_dataset_name }}.fact_total_movement
(
total_movement_sid,
date_id	,
employee_sid	,
employee_num	,
action_code	,
action_reason_text	,
analytics_msr_sid	,
movement_type	,
movement_direction_text	,
from_position_sid	,
from_position_level_sequence_num	,
from_fte_pct	,
from_schedule_work_code  ,
from_working_location_code	,
from_dept_sid	,
from_lawson_company_num	,
from_process_level_code	,
from_job_code_sid	,
from_job_class_sid	,
from_coid	,
from_company_code	,
from_sub_functional_dept_num	,
from_functional_dept_num	,
from_auxiliary_status_sid	,
from_employee_status_sid	,
from_pay_rate_amt	,
from_key_talent_id,
from_integrated_lob_id,
to_position_sid	,
to_position_level_sequence_num	,
to_fte_pct	,
to_schedule_work_code  ,
to_working_location_code	,
to_dept_sid	,
to_lawson_company_num	,
to_process_level_code	,
to_job_code_sid	,
to_job_class_sid	,
to_coid	,
to_company_code	,
to_sub_functional_dept_num	,
to_functional_dept_num	,
to_auxiliary_status_sid	,
to_employee_status_sid	,
to_pay_rate_amt	,
to_key_talent_id,
to_integrated_lob_id,
pay_change_diff_amt	,
source_system_code	,
dw_last_update_date_time)

select distinct
		(select coalesce(max(total_movement_sid),0)  from  {{ params.param_hr_base_views_dataset_name }}.fact_total_movement)
		+ row_number() over ( order by coalesce(start_employee_sid,end_employee_sid)) as total_movement_sid,
		 coalesce(end_date_id,start_date_id) as date_id
		,coalesce(start_employee_sid,end_employee_sid) as employee_sid
		,coalesce(start_employee_num,end_employee_num) as employee_num
		,pa.action_code
		,pa.action_reason_text
		,80700 as analytics_msr_sid
		,'Internal' as monvement_type
		,case when pa.action_code like '%XFER%' and start_position_sid <> end_position_sid
			and end_pay_rate_amt > 1 
			and start_pay_rate_amt > 1 
			and (end_pay_rate_amt - start_pay_rate_amt)/start_pay_rate_amt >= .02 then 'Promotion'
	  when pa.action_code like '%XFER%' and start_position_sid <> end_position_sid
			and end_pay_rate_amt > 1 
			and start_pay_rate_amt > 1 
			and (end_pay_rate_amt - start_pay_rate_amt)/start_pay_rate_amt<= -.02 then 'Demotion'
	when pa.action_code like '%XFER%' and start_position_sid <> end_position_sid
			and end_pay_rate_amt > 1 
			and start_pay_rate_amt > 1 
			and (end_pay_rate_amt - start_pay_rate_amt)/start_pay_rate_amt > -.02
			and (end_pay_rate_amt - start_pay_rate_amt)/start_pay_rate_amt < .02 then 'Lateral'
		else 'No_Position_Change' end as movement_direction
		,start_position_sid as from_position_sid
		,start_position_level_sequence_num as from_position_level_sequence_num
		,start_fte_percent as from_fte_percent
		,start_schedule_work_code as from_schedule_work_code
		,start_working_location_code as from_working_location_code
		,start_dept_sid as from_dept_sid
		,start_lawson_company_num as from_lawson_company_num
		,start_process_level_code as from_process_level_code
		,start_job_code_sid as from_job_code_sid
		,start_job_class_sid as from_job_class_sid
		,start_coid as from_coid
		,start_company_code as from_company_code
		,cast(start_sub_functional_dept_num as int64) as from_sub_functional_dept_num
		,cast(start_functional_dept_num as int64) as from_functional_dept_num
		,start_auxiliary_status_sid as from_auxiliary_status_sid
		,start_employee_status_sid as from_employee_status_sid
		,start_pay_rate_amt as from_pay_rate_amt
		,start_key_talent_id as from_key_talent_id
		,start_integrated_lob_id as from_integrated_lob_id
		,end_position_sid as to_position_sid
		,end_position_level_sequence_num as to_position_level_sequence_num
		,end_fte_percent as to_fte_percent
		,end_schedule_work_code as to_schedule_work_code
		,end_working_location_code as to_working_location_code
		,end_dept_sid as to_dept_sid
		,end_lawson_company_num as to_lawson_company_num
		,end_process_level_code as to_process_level_code
		,end_job_code_sid as to_job_code_sid
		,end_job_class_sid as to_job_class_sid
		,end_coid as to_coid
		,end_company_code as to_company_code
		,cast(end_sub_functional_dept_num as int64) as to_sub_functional_dept_num
		,cast(end_functional_dept_num as int64) as to_functional_dept_num
		,end_auxiliary_status_sid as to_auxiliary_status_sid
		,end_employee_status_sid as to_employee_status_sid
		,end_pay_rate_amt as to_pay_rate_amt
		,end_key_talent_id as to_key_talent_id
		,end_integrated_lob_id as to_integrated_lob_id
		,(end_pay_rate_amt - start_pay_rate_amt)/start_pay_rate_amt as pay_change_diff
		,'L' source_system_code
		,timestamp_trunc( current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
from step_2 st

inner join pos_action pa
		on coalesce(start_employee_sid,end_employee_sid) = pa.employee_sid
		and coalesce(end_date_id,start_date_id) = pa.eff_from_date

where coalesce(end_date_id,start_date_id) >= '2016-01-01';

  /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select total_movement_sid, employee_sid	
        from  {{ params.param_hr_core_dataset_name }}.fact_total_movement
        group by total_movement_sid, employee_sid
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      drop table pos_date;
      drop table step_2;
      drop table pos_action;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table:  {{ params.param_hr_core_dataset_name }}.fact_total_movement');
    ELSE
      COMMIT TRANSACTION;
    END IF;
    
--- drop temp tables
drop table pos_date;
drop table step_2;
drop table pos_action;


end;