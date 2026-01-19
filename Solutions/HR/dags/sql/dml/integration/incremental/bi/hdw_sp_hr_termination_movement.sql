begin

declare  dup_count int64;
declare current_ts datetime;
set current_ts = current_datetime('US/Central');

create temporary table pos_date as (
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
		d.date_id,
		pa.action_code,
		pa.action_reason_text
from {{ params.param_hr_base_views_dataset_name }}.employee_position ep

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
						case when min(eff_from_date) over (partition by company_pay_schedule_sid,pay_grade_code,lawson_company_num order by eff_from_date rows between 1 following and 1 following) is null then cast ('12/31/9999' as date format 'mm/dd/yyyy')
						else (min(eff_from_date) over (partition by company_pay_schedule_sid,pay_grade_code,lawson_company_num order by eff_from_date rows between 1 following and 1 following))-1 end as eff_to_date,
						pay_schedule_code,
						pay_rate_amt, 
						lawson_company_num
				from	{{ params.param_hr_base_views_dataset_name }}.company_pay_grade_schedule
				where date(valid_to_date)='9999-12-31'
				and pay_step_num = 2
				) pay
		on jp.pay_grade_code = pay.pay_grade_code
		and jp.pay_grade_schedule_code = pay.pay_schedule_code
		and jp.lawson_company_num = pay.lawson_company_num
		and jp.eff_from_date between pay.eff_from_date and pay.eff_to_date

inner join (select date_id from {{ params.param_pub_views_dataset_name }}.lu_date where date_id < current_date ) d
	on d.date_id between ep.eff_from_date and ep.eff_to_date

left join {{ params.param_hr_stage_dataset_name }}.aux_status s
		on ep.employee_sid = s.employee_sid
		and d.date_id between s.status_from_date and s.status_to_date
		and upper(trim(s.aux_status_code)) in ('PRN','FT','PT','TEMP')

inner join {{ params.param_hr_stage_dataset_name }}.emp_status es
		on ep.employee_sid = es.employee_sid
		and d.date_id between es.status_from_date and es.status_to_date
		and upper(trim(es.emp_status_code)) in ('01','02','03','04','05')
		
left outer join {{ params.param_hr_base_views_dataset_name }}.person_action pa
		on ep.employee_sid = pa.employee_sid
		and ep.eff_from_date = pa.eff_from_date
		and pa.valid_to_date = datetime("9999-12-31 23:59:59")
		and upper(trim(pa.action_code)) like '%HIRE%'
		and upper(trim(pa.action_code)) not like '%99%'

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
);

create temporary table min_pos as (
select distinct 
		ep.employee_sid, 
		ep.position_sid, 
		ep.employee_num,
		ep.position_level_sequence_num,
		ep.eff_to_date-ep.eff_from_date as pos_length,
		ep.eff_from_date as start_date
from {{ params.param_hr_base_views_dataset_name }}.employee_position ep
where ep.valid_to_date = datetime("9999-12-31 23:59:59")
		and ep.lawson_company_num <> 300
    );

BEGIN TRANSACTION;
-- delete prior data for movement
delete
from {{ params.param_hr_core_dataset_name }}.fact_total_movement
where analytics_msr_sid in (
select analytics_msr_sid from  {{ params.param_dim_base_views_dataset_name }}.dim_analytics_measure
where upper(trim(analytics_msr_name_child)) ='HR_LAWSON_TERMINATIONS')
and (to_lawson_company_num <> 300
or from_lawson_company_num <> 300);		

-- insert movement data

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

select (select coalesce(max(total_movement_sid),0)  from  {{ params.param_hr_base_views_dataset_name }}.fact_total_movement)
+ row_number() over ( order by pa.employee_sid) as total_movement_sid,
pa.eff_from_date as date_id,
pa.employee_sid,
pa.employee_num,
pa.action_code,
pa.action_reason_text,
80300 as movement_analytics_sid,
case when pos_date.position_sid is null then 'External' else 'Internal' end as movement_type,
case when pos_date.pay_rate_amt > 1 
			and pay.pay_rate_amt > 1 
			and (pos_date.pay_rate_amt - pay.pay_rate_amt)/pay.pay_rate_amt >= .02 then 'Promotion'
		when pos_date.pay_rate_amt > 1 
			and pay.pay_rate_amt > 1 
			and (pos_date.pay_rate_amt - pay.pay_rate_amt)/pay.pay_rate_amt <= -.02 then 'Demotion'
		when pos_date.pay_rate_amt > 1 
			and pay.pay_rate_amt > 1 
			and (pos_date.pay_rate_amt - pay.pay_rate_amt)/pay.pay_rate_amt > -.02
			and (pos_date.pay_rate_amt - pay.pay_rate_amt)/pay.pay_rate_amt < .02 then 'Lateral'
		else null end as movement_direction,
pa.position_sid as from_position_sid,
pa.position_level_sequence_num as from_position_level_sequence_num,
pa.fte_percent as from_fte_percent,
pa.schedule_work_code as from_schedule_work_code,
pa.working_location_code as from_working_location_code,
pa.dept_sid as from_dept_sid,
pa.lawson_company_num as from_lawson_company_num,
pa.process_level_code as from_process_level_code,
jc.job_code_sid as from_job_code_sid,
jc.job_class_sid as from_job_class_sid,
gldc.coid as from_coid,
gldc.company_code as from_company_code,
cast(sf.sub_functional_dept_num as int64) as from_sub_functional_dept_num,
cast(sf.functional_dept_num as int64) as from_functional_dept_num,
coalesce(s.aux_status_sid, pa.status_sid) as from_auxiliary_status_sid,
es.emp_status_sid as from_employee_status_sid,
pay.pay_rate_amt as from_pay_rate_amt,
coalesce(rkeyt1.key_talent_id,rkeyt2.key_talent_id,rkeyt3.key_talent_id,rkeyt4.key_talent_id,rkeyt5.key_talent_id,rkeyt6.key_talent_id,rkeyt7.key_talent_id,rkeyt8.key_talent_id) as to_key_talent_id,
coalesce(mat1.integrated_lob_id,mat4.integrated_lob_id,mat3.integrated_lob_id,mat2.integrated_lob_id) as to_integrated_lob_id,
pos_date.position_sid as to_position_sid, 
pos_date.position_level_sequence_num as to_position_level_sequence_num, 
pos_date.fte_percent as to_fte_percent,
pos_date.schedule_work_code as to_schedule_work_code,
pos_date.working_location_code as to_working_location_code, 
pos_date.dept_sid as to_dept_sid, 
pos_date.lawson_company_num as to_lawson_company_num, 
pos_date.process_level_code as to_process_level_code,
pos_date.job_code_sid as to_job_code_sid,
pos_date.job_class_sid as to_job_class_sid,
pos_date.coid as to_coid,
pos_date.company_code as to_company_code,
cast(pos_date.sub_functional_dept_num as int64) as to_sub_functional_dept_num,
cast(pos_date.functional_dept_num as int64) as to_functional_dept_num,
pos_date.auxiliary_status_sid as to_auxiliary_status_sid,
pos_date.employee_status_sid as to_employee_status_sid,
pos_date.pay_rate_amt as to_pay_rate_amt,
pos_date.key_talent_id as from_key_talent_id,
pos_date.integrated_lob_id as from_integrated_lob_id,
(pos_date.pay_rate_amt - pay.pay_rate_amt)/pay.pay_rate_amt as pay_change_diff,
'L' source_system_code,
timestamp_trunc( current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
from  (

    select 
    paterm.employee_sid,
    paterm.action_code,
    paterm.employee_num,
    paterm.lawson_company_num,
    paterm.action_reason_text,
    paterm.eff_from_date,
    paterm.action_last_update_date,
    paterm.valid_to_date,
    ep.working_location_code,
    ep.process_level_code,
    ep.position_sid,
	  ep.position_level_sequence_num,
  	ep.fte_percent,
	  ep.schedule_work_code,
    ep.dept_sid,
    ep.gl_company_num,
    ep.account_unit_num,
    jes.status_sid
    
    from {{ params.param_hr_base_views_dataset_name }}.person_action paterm
    
    inner join {{ params.param_hr_base_views_dataset_name }}.employee e
		    on e.employee_sid = paterm.employee_sid
		    and e.valid_to_date = datetime("9999-12-31 23:59:59")
    
    left join {{ params.param_hr_base_views_dataset_name }}.junc_employee_status jes
		    on e.employee_sid = jes.employee_sid
		    and jes.status_type_code = 'AUX'
		    and jes.valid_to_date = datetime("9999-12-31 23:59:59")
    
    inner join {{ params.param_hr_base_views_dataset_name }}.employee_position ep
		    on ep.employee_sid = paterm.employee_sid
		    and ep.valid_to_date = datetime("9999-12-31 23:59:59")
		    and paterm.eff_from_date = ep.eff_to_date
    
    where paterm.valid_to_date = datetime("9999-12-31 23:59:59")
		    and paterm.action_code like '%1TERM%'
			and paterm.action_code <> '9TERMDVEST'
		    and paterm.eff_from_date >= '2016-01-01'

    
    qualify row_number()over(partition by paterm.employee_num, paterm.lawson_company_num, paterm.eff_from_date order by ep.position_level_sequence_num, paterm.action_last_update_date desc) = 1 --only taking the most updated action
    
    union all
    
    select 
    paxfer.employee_sid,
    paxfer.action_code,
    paxfer.employee_num,
    paxfer.lawson_company_num,
    paxfer.action_reason_text,
    paxfer.eff_from_date,
    paxfer.action_last_update_date,
    paxfer.valid_to_date,
    ep1.working_location_code,
    ep1.process_level_code,
    ep1.position_sid,
	  ep1.position_level_sequence_num,
	  ep.fte_percent,
	  ep.schedule_work_code,
    ep1.dept_sid,
    ep1.gl_company_num,
    ep1.account_unit_num,
    jes.status_sid
    
    from {{ params.param_hr_base_views_dataset_name }}.person_action paxfer
    
    inner join {{ params.param_hr_base_views_dataset_name }}.employee e
		    on e.employee_sid = paxfer.employee_sid
		    and e.valid_to_date = datetime("9999-12-31 23:59:59")
    
    left join {{ params.param_hr_base_views_dataset_name }}.junc_employee_status jes
		    on e.employee_sid = jes.employee_sid
		    and jes.status_type_code = 'AUX'
		    and jes.valid_to_date = datetime("9999-12-31 23:59:59")
    
inner join
    
    (select
    employee_sid,
    position_sid,
    lawson_company_num,
    process_level_code,
    eff_from_date,
    eff_to_date,
    working_location_code,
	  position_level_sequence_num,
    dept_sid,
    gl_company_num,
    account_unit_num,
    row_number()over(partition by employee_sid, position_level_sequence_num order by  --added position_level_sequence_num
    position_level_sequence_num, eff_from_date) as row_rank
    
    from (
        
		        select    
		        employee_sid,
		        position_sid,
		        lawson_company_num,
		        process_level_code,
		        working_location_code,
		        dept_sid,
		        gl_company_num,
		        account_unit_num,
		        position_level_sequence_num,        --thomas change
		        eff_from_date,        --thomas change
		        eff_to_date        --thomas change
		    
		    from {{ params.param_hr_base_views_dataset_name }}.employee_position
		    
		    where valid_to_date = datetime("9999-12-31 23:59:59")
		    --group by 1,2,3,4,5,6,7,8        --thomas change
		    ) a 
    ) ep2
    
    on ep2.employee_sid = paxfer.employee_sid
    and ep2.eff_from_date = paxfer.eff_from_date
    
    inner join (
    select
    employee_sid,
    position_sid,
    lawson_company_num,
    process_level_code,
    eff_from_date,
    eff_to_date,
    working_location_code,
    dept_sid,
	position_level_sequence_num,
    gl_company_num,
    account_unit_num,
    row_number()over(partition by employee_sid, position_level_sequence_num order by  --added position_level_sequence_num
    position_level_sequence_num, eff_from_date) as row_rank
    from (
    
				    select
				    
				    employee_sid,
				    position_sid,
				    lawson_company_num,
				    process_level_code,
				    working_location_code,
				    dept_sid,
				    gl_company_num,
				    account_unit_num,
				    position_level_sequence_num,        --thomas change
				    eff_from_date,        --thomas change
				    eff_to_date        --thomas change
				    
				    from {{ params.param_hr_base_views_dataset_name }}.employee_position
				    
				    where valid_to_date = datetime("9999-12-31 23:59:59")
				    --group by 1,2,3,4,5,6,7,8        --thomas change
				    ) a
    ) ep1
    
		    on ep1.employee_sid = paxfer.employee_sid
		    and paxfer.eff_from_date - 1 between ep1.eff_from_date and ep1.eff_to_date
			
	left join {{ params.param_hr_base_views_dataset_name }}.employee_position ep
			on ep1.employee_sid = ep.employee_sid
			and ep1.position_sid = ep.position_sid
			and ep1.position_level_sequence_num = ep.position_level_sequence_num
			and ep.eff_from_date = ep1.eff_from_date
			and ep.valid_to_date = datetime("9999-12-31 23:59:59")
    
    where paxfer.action_code like '%1XFER%'
		    and paxfer.valid_to_date = datetime("9999-12-31 23:59:59")
		    and paxfer.eff_from_date >= '2016-01-01'
		    and ep1.row_rank + 1 = ep2.row_rank
		    and ep1.process_level_code <> ep2.process_level_code
	
    
    qualify row_number()over(partition by paxfer.employee_num, paxfer.lawson_company_num, paxfer.eff_from_date order by ep1.position_level_sequence_num, paxfer.action_last_update_date desc) = 1 
) pa

inner join {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk gldc
		on pa.gl_company_num = gldc.gl_company_num
		and pa.account_unit_num = gldc.account_unit_num
		and gldc.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.job_position jp
		on pa.position_sid = jp.position_sid
		and jp.valid_to_date = datetime("9999-12-31 23:59:59")
		and pa.eff_from_date between jp.eff_from_date and jp.eff_to_date

inner join {{ params.param_hr_base_views_dataset_name }}.job_code jc
		on jp.job_code_sid = jc.job_code_sid
		and jc.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department sf
		on sf.dept_num = gldc.dept_num
		and sf.coid = gldc.coid
		and sf.company_code = gldc.company_code

left join {{ params.param_hr_stage_dataset_name }}.aux_status s
		on pa.employee_sid = s.employee_sid
		and pa.eff_from_date between s.status_from_date and s.status_to_date
		and s.aux_status_code in ('PRN','FT','PT','TEMP')

inner join {{ params.param_hr_stage_dataset_name }}.emp_status es
		on pa.employee_sid = es.employee_sid
		and pa.eff_from_date -1 between es.status_from_date and es.status_to_date
		and es.emp_status_code in ('01','02','03','04','05')

left join {{ params.param_pub_views_dataset_name }}.fact_facility ff
		on gldc.coid = ff.coid 
		and gldc.company_code = ff.company_code 

left join (
		    select
		    lawson_location_code,
		    hospital_category_code,
		    hospital_category_code_year
		    from {{ params.param_hr_views_dataset_name }}.ref_lawson_location_hospital_category
		) rllhc1

		on extract(year from pa.eff_from_date) = rllhc1.hospital_category_code_year
		and pa.working_location_code = rllhc1.lawson_location_code

left join (
		    select
		    lawson_location_code,
		    hospital_category_code,
		    hospital_category_code_year
		    from  {{ params.param_hr_base_views_dataset_name }}.ref_lawson_location_hospital_category 
		    qualify row_number()over(partition by lawson_location_code order by hospital_category_code_year desc)=1
		) rllhc
		on pa.working_location_code = rllhc.lawson_location_code

--modified by thomas jones 12/16
left outer join (
				select	company_pay_schedule_sid, 
						pay_grade_code, 
						pay_step_num,
						eff_from_date, 
						case when min(eff_from_date) over (partition by company_pay_schedule_sid,pay_grade_code,lawson_company_num order by eff_from_date rows between 1 following and 1 following) is null then cast ('12/31/9999' as date format 'mm/dd/yyyy')
						else (min(eff_from_date) over (partition by company_pay_schedule_sid,pay_grade_code,lawson_company_num order by eff_from_date rows between 1 following and 1 following))-1 end as eff_to_date,
						pay_schedule_code,
						pay_rate_amt, 
						lawson_company_num
				from	{{ params.param_hr_base_views_dataset_name }}.company_pay_grade_schedule
				where valid_to_date = datetime("9999-12-31 23:59:59")
				and pay_step_num = 2
				) pay
		on jp.pay_grade_code = pay.pay_grade_code
		and jp.pay_grade_schedule_code = pay.pay_schedule_code
		and jp.lawson_company_num = pay.lawson_company_num
		and pa.eff_from_date between pay.eff_from_date and pay.eff_to_date

left outer join {{ params.param_pub_views_dataset_name }}.functional_department df   ---------------------------thomas add for ilob-----------------------------------
		on sf.functional_dept_num = df.functional_dept_num

left join {{ params.param_hr_base_views_dataset_name }}.department dept   ---------------------------thomas add for ilob-----------------------------------
		on pa.dept_sid = dept.dept_sid
		and dept.valid_to_date = datetime("9999-12-31 23:59:59")

		------Intergrated LOB Mapping Start----------------------------------------------------------------------------------------------------------------------------------------------------


LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat1  --Process Level AND Dept Num
ON pa.process_level_code = mat1.process_level_code AND dept.dept_code = mat1.dept_code
AND mat1.match_level_num = 1

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat2  --LOB and Sub LOB
ON ff.lob_code = mat2.lob_code AND ff.sub_lob_code = mat2.sub_lob_code
AND mat2.match_level_num = 2

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat3  --Function and Sub Function
ON sf.functional_dept_desc = mat3.functional_dept_desc
AND sf.sub_functional_dept_desc = mat3.sub_functional_dept_desc
AND mat3.match_level_num = 3

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat4  --Process Level
ON pa.process_level_code = mat4.process_level_code
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
AND pa.process_level_code=rkeyt4.process_level_code

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt5
ON rkeyt5.match_level_num = 5
AND jc.job_code=rkeyt5.job_code
AND pa.process_level_code=rkeyt5.process_level_code

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt6
ON rkeyt6.match_level_num = 6
AND jc.job_code=rkeyt6.job_code
AND pa.process_level_code=rkeyt6.process_level_code
AND jp.position_code_desc LIKE 'Dir Prgm%'

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt7
ON rkeyt6.match_level_num = 7
AND jc.job_code=rkeyt7.job_code
AND pa.process_level_code=rkeyt7.process_level_code
AND dept.dept_code BETWEEN '70000' AND '79999'

LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent rkeyt8
ON rkeyt8.match_level_num = 8
AND jc.job_code=rkeyt8.job_code
AND jc.job_code_desc=rkeyt8.job_code_desc

left outer join min_pos
		on pa.employee_num = min_pos.employee_num
		and pa.position_level_sequence_num = min_pos.position_level_sequence_num
		and ((upper(pa.action_code) like '%TERM%' and pa.eff_from_date between min_pos.start_date-1 and min_pos.start_date) or (upper(pa.action_code) not like '%TERM%' and pa.eff_from_date = min_pos.start_date))

left outer join pos_date
		on min_pos.employee_num = pos_date.employee_num
		and min_pos.start_date = pos_date.eff_from_date
		and min_pos.position_level_sequence_num = pos_date.position_level_sequence_num
		and (upper(pa.action_code) = '1TERMPEND' and (upper(pos_date.action_code) like '%HIRE%' and pos_date.action_code not like '%99%') 
			  or (upper(pa.action_code) like '%1XFER%' and pa.employee_sid = pos_date.employee_sid))


where pa.lawson_company_num <> 300
and pa.eff_from_date >= '2016-01-01'

qualify row_number() over(partition by pa.employee_sid, pa.position_sid, pa.eff_from_date order by pa.action_last_update_date desc,min_pos.start_date desc) = 1;


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
      drop table min_pos;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table:  {{ params.param_hr_core_dataset_name }}.fact_total_movement');
    ELSE
      COMMIT TRANSACTION;
    END IF;
    

--- drop volatile tables
drop table pos_date;
drop table min_pos;

END;
