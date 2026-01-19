begin

DECLARE
  current_ts datetime;
  set current_ts = timestamp_trunc( current_datetime('US/Central'), SECOND);

create temp table bc as ( 

select
s.candidate_profile_sid,
rss.submission_status_name,
case when upper(trim(rss.submission_status_name)) in ('BACKGROUND REQUIRED', 'BACKGROUND REQUIRED TRANSFER LICENSE ONLY', 'BACKGROUND REQUIRED, TRANSFER EDUCATION ONLY', 'BACKGROUND REQUIRED, TRANSFER EDUCATION & LICENSE ONLY', 'FULL BCK_NO DRG', 'FULL BCK & DRUG REQ') THEN 'Y'
when upper(trim(rss.submission_status_name)) in ('NO BACKGROUND REQUIRED', 'NO BCK_DRG', 'HRSM CAND WITHDREW') THEN 'N' 
end as background_check_ind

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
on s.candidate_profile_sid = st.candidate_profile_sid
and st.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
on st.submission_tracking_sid = sts.submission_tracking_sid
and sts.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
on rss.submission_status_id = sts.submission_status_id

where upper(trim(rss.submission_status_name)) in ('BACKGROUND REQUIRED', 'BACKGROUND REQUIRED TRANSFER LICENSE ONLY', 'BACKGROUND REQUIRED, TRANSFER EDUCATION ONLY', 'BACKGROUND REQUIRED, TRANSFER EDUCATION & LICENSE ONLY', 'FULL BCK_NO DRG', 'FULL BCK & DRUG REQ', 'NO BACKGROUND REQUIRED', 'NO BCK_DRG', 'HRSM CAND WITHDREW')
and s.valid_to_date = datetime("9999-12-31 23:59:59")

qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, st.submission_tracking_num desc) =1); --pulling latest record for each candidate from submission_tracking

--table only loads candidates who went through the adjudication process (something came up in background screening so needed to work through concerns)
create temp table adj as (

select
s.candidate_profile_sid,
rss.submission_status_name,
case when upper(trim(rss.submission_status_name)) = 'DECISION PENDING' then 'Y' else 'N' end as adjudication_ind

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
on s.candidate_profile_sid = st.candidate_profile_sid
and st.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
on st.submission_tracking_sid = sts.submission_tracking_sid
and sts.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
on rss.submission_status_id = sts.submission_status_id

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(rss.submission_status_name)) = 'DECISION PENDING'

qualify row_number()over(partition by s.candidate_profile_sid order by st.event_date_time, st.submission_tracking_num desc) =1); --pulling latest record for each candidate from submission_tracking

--only candidates with any status of preadverse (notifies candidate of potentially negative info from their background screening) are loaded here
create temp table pa as ( 

select
s.candidate_profile_sid,
rss.submission_status_name,
case when upper(trim(rss.submission_status_name)) = 'HRSM PREADVERSE' THEN 'Y' ELSE 'N'  end as preadverse_ind

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
on s.candidate_profile_sid = st.candidate_profile_sid
and st.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
on st.submission_tracking_sid = sts.submission_tracking_sid
and sts.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
on rss.submission_status_id = sts.submission_status_id

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(rss.submission_status_name)) = 'HRSM PREADVERSE' 

qualify row_number()over(partition by s.candidate_profile_sid order by st.event_date_time, st.submission_tracking_num desc) =1); --pulling latest record for each candidate from submission_tracking

--only candidates with confirmed onboarding are captured here
create temp table oc as ( 

select
s.candidate_profile_sid,
rss.submission_status_name,
co.onboarding_confirmation_date,
case when upper(trim(rss.submission_status_name)) = 'CONFIRMED COMPLETED' THEN 'Y'
when co.onboarding_confirmation_date is not null then 'Y'
else 'N' end as onboard_confirm_ind

from 
{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
on s.candidate_profile_sid = st.candidate_profile_sid
and st.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
on st.submission_tracking_sid = sts.submission_tracking_sid
and sts.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
on rss.submission_status_id = sts.submission_status_id

left join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding co --joining to enwisen
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid
and co.valid_to_date = datetime("9999-12-31 23:59:59")

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and ( upper(trim(rss.submission_status_name)) = 'CONFIRMED COMPLETED' 
OR co.onboarding_confirmation_date is not null) 

qualify row_number()over(partition by s.candidate_profile_sid order by st.event_date_time, st.submission_tracking_num desc) =1); --pulling latest record for each candidate from submission_tracking

--only candidates with a completed drug screen are loaded here
create temp table ds as (

select
s.candidate_profile_sid,
rss.submission_status_name,
coe.completed_date,
case when upper(trim(rss.submission_status_name)) = 'DRUG SCREEN COMPLETED' THEN 'Y'
when coe.completed_date is not null then 'Y'
else 'N' end as drug_screen_ind

from 
{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
on s.candidate_profile_sid = st.candidate_profile_sid
and st.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
on st.submission_tracking_sid = sts.submission_tracking_sid
and sts.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
on rss.submission_status_id = sts.submission_status_id

left join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding co --joining to enwisen
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid
and co.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event coe --candidates are only loaded here if their drug screen is completed
on rr.recruitment_requisition_num_text = coe.recruitment_requisition_num_text
--and s.candidate_sid = coe.candidate_sid
and coe.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type ro
on coe.event_type_id = cast(ro.event_type_id as string)

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ro.event_type_desc)) = 'DRUG SCREEN COMPLETION'
and ( upper(trim(rss.submission_status_name)) = 'DRUG SCREEN COMPLETED'
OR coe.completed_date is not null)

qualify row_number()over(partition by s.candidate_profile_sid order by st.event_date_time, st.submission_tracking_num desc) =1); --pulling latest record for each candidate from submission_tracking

--only contains candidates with a 1hrrescind record. this can only happen once a candidate is in the hroc process (round 2 or later)
create temp table rs as (

select
s.candidate_profile_sid,
rss.submission_status_name,
case when upper(trim(rss.submission_status_name)) in ('1HR RESCIND OFFER',  'RESCIND OFFER') THEN 'Y'
else 'N' end as offer_rescind_ind

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
on s.candidate_profile_sid = st.candidate_profile_sid
and st.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
on st.submission_tracking_sid = sts.submission_tracking_sid
and sts.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
on rss.submission_status_id = sts.submission_status_id

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(rss.submission_status_name)) in ('1HR RESCIND OFFER',  'RESCIND OFFER')

qualify row_number()over(partition by s.candidate_profile_sid order by st.event_date_time, st.submission_tracking_num desc) =1); --pull candidate's latest record from st


--only contains candidates where the pre-employment screen has been initiated
create temp table ps as (

select
s.candidate_profile_sid,
rss.submission_status_name,
case when upper(trim(rss.submission_status_name)) in ('PRE-EMPLOYMENT SCREEN INITIATED', 'PREEMP SCREEN INIT')THEN 'Y'
else 'N' end as prescreen_ind

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
on s.candidate_profile_sid = st.candidate_profile_sid
and st.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
on st.submission_tracking_sid = sts.submission_tracking_sid
and sts.valid_to_date = datetime("9999-12-31 23:59:59")

left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
on rss.submission_status_id = sts.submission_status_id

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(rss.submission_status_name)) in ('PRE-EMPLOYMENT SCREEN INITIATED', 'PREEMP SCREEN INIT')

qualify row_number()over(partition by s.candidate_profile_sid order by st.event_date_time, st.submission_tracking_num desc) =1) ;--pull candidate's latest record from st


--captures each microstep a candidate moves through during the onboarding process
create temp table ms as (

--offer extend to offer accepted (recruiting)
select
s.candidate_profile_sid,
1 as microstep_num,
coalesce(st1.event_date_time, st1.creation_date_time) as start_date_time, 
coalesce(st2.event_date_time, st2.creation_date_time) as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --gives us most recent offer
) o
on s.submission_sid = o.submission_sid

left join (

	select
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.creation_date_time,
	rss.submission_status_name

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(rss.submission_status_name)) = 'EXTENDED'
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
)st1

on st1.candidate_profile_sid = s.candidate_profile_sid
and o.extend_date = coalesce(cast(st1.event_date_time as date), cast(st1.creation_date_time as date)) --because there can be multiple offer dates in st, this picks the one that matches the offer date in offer table

left join (

	select
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.creation_date_time,
	rss.submission_status_name,
	st.submission_tracking_num

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(rss.submission_status_name)) = 'ACCEPTED'
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
)st2

on st2.candidate_profile_sid = s.candidate_profile_sid
and o.accept_date = coalesce(cast(st2.event_date_time as date), cast(st2.creation_date_time as date)) --because there can be multiple accept dates in st, this picks the one that matches the accept date in offer table

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(st1.event_date_time, st1.creation_date_time) is not null 
and coalesce(st2.event_date_time, st2.creation_date_time) is not null
and cast(coalesce(st1.event_date_time, st1.creation_date_time) as date) > date_add(cast(current_ts as date), interval -13 month)
and coalesce(st1.event_date_time, st1.creation_date_time) <= coalesce(st2.event_date_time, st2.creation_date_time)

qualify row_number()over(partition by s.candidate_profile_sid order by coalesce(st1.event_date_time, st1.creation_date_time)  desc, end_date_time desc, st2.submission_tracking_num desc)=1 --if a submission has the same status multiple times, this pulls the last record

union all

--offer acceptance to round 2, or if taleo after 12/8/21, background authorization
select
s.candidate_profile_sid,
2 as microstep_num,
coalesce(st1.event_date_time, st1.creation_date_time) as start_date_time, 
case when cast(s4.creation_date_time as date) >= '2021-12-09' and upper(trim(s4.source_system_code)) = 'T'
then co.completed_date
when cast(s4.creation_date_time as date) >= '2022-03-10' and upper(trim(s4.source_system_code)) = 'B'
then co.completed_date --hdm-2076
else coalesce(s2.event_date_time, s3.creation_date_time) --pulling taleo's event_date_time and ats's creation_date_time since ats is always null for event_date_time. 
end as end_date_time,
'C' as day_type_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls the latest offer
) o
on s.submission_sid = o.submission_sid

left join (

	select
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	rss.submission_status_name,
	st.submission_tracking_num,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(rss.submission_status_name)) = 'ACCEPTED'
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')

)st1

on s.candidate_profile_sid = st1.candidate_profile_sid
and o.accept_date = coalesce(cast(st1.event_date_time as date), cast(st1.creation_date_time as date)) --because there can be multiple accept dates in st, this picks the one that matches the accept date in offer table

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'T' --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'B' --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time asc, st.event_date_time asc, submission_tracking_num asc)=1 --if a submission has the same step multiple times, this pulls the first record
)s3

on s.candidate_profile_sid = s3.candidate_profile_sid

left join (
	select
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	rss.submission_status_name,
	st.submission_tracking_num,
	st.creation_date_time,
	st.source_system_code

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(rss.submission_status_name)) = 'EXTENDED'
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
)s4

on s.candidate_profile_sid = s4.candidate_profile_sid
and o.extend_date = coalesce(cast(s4.event_date_time as date), cast(s4.creation_date_time as date)) --pulls date offer was extended because any done after 12/8 for taleo have different logic applied (hdm-2047)

left join (
	select 
	cano.candidate_sid,
	cano.requisition_sid,
	coe.completed_date

	from {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding cano --joining to enwisen data

	inner join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event coe
	on cano.recruitment_requisition_num_text = coe.recruitment_requisition_num_text
	--and s.candidate_sid = coe.candidate_sid
	and coe.valid_to_date = datetime("9999-12-31 23:59:59")

	inner join {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type ro
	on coe.event_type_id = cast(ro.event_type_id as string)

	where cano.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(ro.event_type_desc)) = 'AUTHORIZED BACKGROUND CHECK'
)co
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and o.accept_date is not null
and coalesce(st1.event_date_time, st1.creation_date_time) <= case when cast(s4.creation_date_time as date) >= '2021-12-09' and s4.source_system_code = 'T'
then co.completed_date
when cast(s4.creation_date_time as date) >= '2022-03-10' and upper(trim(s4.source_system_code)) = 'B'
then co.completed_date --hdm-2076
else coalesce(s2.event_date_time, s3.creation_date_time) --pulling taleo's event_date_time and ats's creation_date_time since ats is always null for event_date_time. 
end
and cast(coalesce(st1.event_date_time, st1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

qualify row_number()over(partition by st1.candidate_profile_sid order by st1.creation_date_time desc, st1.event_date_time desc, st1.submission_tracking_num desc)=1

union all

--round 2 to  pre employment screen initiated
select
s.candidate_profile_sid,
3 as microstep_num,
coalesce(s2.event_date_time, s3.creation_date_time) as start_date_time, --pulling taleo's event_date_time and ats's creation_date_time since ats is always null for event_date_time
coalesce(s1.event_date_time, s1.creation_date_time) as end_date_time, 
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('PRE-EMPLOYMENT SCREEN INITIATED', 'PREEMP SCREEN INIT')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'T' --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'B' --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time asc, st.event_date_time asc, submission_tracking_num asc)=1 --if a submission has the same step multiple times, this pulls the first record
)s3

on s.candidate_profile_sid = s3.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s2.event_date_time, s3.creation_date_time) <= coalesce(s1.event_date_time, s1.creation_date_time)
and cast(coalesce(s2.event_date_time, s3.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--round 2 to preadverse
select
s.candidate_profile_sid,
4 as microstep_id,
coalesce(s1.event_date_time, s2.creation_date_time) as start_date_time, --pulling taleo's event_date_time and ats's creation_date_time since ats is always null for event_date_time
coalesce(s3.event_date_time, s3.creation_date_time) as end_date_time, 
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'T' --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'B' --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time asc, st.event_date_time asc, submission_tracking_num asc)=1 --if a submission has the same step multiple times, this pulls the first record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) = 'HRSM PREADVERSE'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s3

on s.candidate_profile_sid = s3.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s1.event_date_time, s2.creation_date_time) <= coalesce(s3.event_date_time, s3.creation_date_time)
and cast(coalesce(s1.event_date_time, s2.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--round 2 to move to hire
select
s.candidate_profile_sid,
5 as microstep_num,
coalesce(s2.event_date_time, s3.creation_date_time) as start_date_time,  --pulling taleo's event_date_time and ats's creation_date_time since ats is always null for event_date_time
coalesce(s1.event_date_time, s1.creation_date_time) as end_date_time, 
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('MOVE TO HIRE', 'CONFIRM PENDING') --there is no 'Move to Hire'' for ats so 'Confirm Pending'' is closest equivalent
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'T' --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'B' --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time asc, st.event_date_time asc, submission_tracking_num asc)=1 --if a submission has the same step multiple times, this pulls the first record
)s3

on s.candidate_profile_sid = s3.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s2.event_date_time, s3.creation_date_time) <= coalesce(s1.event_date_time, s1.creation_date_time)
and cast(coalesce(s2.event_date_time, s3.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--pre employment screen initiated to background check complete
select
s.candidate_profile_sid,
6 as microstep_num,
coalesce(s1.event_date_time, s1.creation_date_time) as start_date_time,
coalesce(s2.event_date_time, s2.creation_date_time) as end_date_time,  
'B' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('PRE-EMPLOYMENT SCREEN INITIATED', 'PREEMP SCREEN INIT')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ( 'BCKGRND CHK COMPLETE', '1HR BACKGROUND CHECK COMPLETE')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the first record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s1.event_date_time, s1.creation_date_time) <= coalesce(s2.event_date_time, s2.creation_date_time)
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--pre employment screen intiatied to drug screen results
select
s.candidate_profile_sid,
7 as microstep_id,
coalesce(s1.event_date_time, s1.creation_date_time) as start_date_time,
coe.completed_date as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from 
{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('PRE-EMPLOYMENT SCREEN INITIATED', 'PREEMP SCREEN INIT')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event coe --joining to enwisen data
on rr.recruitment_requisition_num_text = coe.recruitment_requisition_num_text
--and s.candidate_sid = coe.candidate_sid
and coe.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type ro
on cast(ro.event_type_id as string) = coe.event_type_id

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ro.event_type_desc)) = 'DRUG SCREEN COMPLETION'
and coalesce(s1.event_date_time, s1.creation_date_time) <= coe.completed_date
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--drug screen results to move to hire
select
s.candidate_profile_sid,
8 as microstep_id,
coe.completed_date as start_date_time,
coalesce(s1.event_date_time, s1.creation_date_time) as end_date_time,
'C' as business_day_code,
rr.source_system_code,
current_ts AS dw_last_update_date_time

from 
{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('MOVE TO HIRE', 'CONFIRM PENDING') 
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the first record

)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event coe --joining to enwisen data
on rr.recruitment_requisition_num_text = coe.recruitment_requisition_num_text
--and s.candidate_sid = coe.candidate_sid
and coe.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type ro
on cast(ro.event_type_id as string) = coe.event_type_id

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ro.event_type_desc)) = 'DRUG SCREEN COMPLETION'
and coe.completed_date <= coalesce(s1.event_date_time, s1.creation_date_time)
and cast(coe.completed_date as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)
qualify row_number()over(partition by s.candidate_profile_sid order by coe.completed_date desc)=1 
--some applicants can have multiple completed dates, so picking the latest one

union all

--drug screen results to rescind
select
s.candidate_profile_sid,
9 as microstep_id,
coe.completed_date as start_date_time,
coalesce(s1.event_date_time, s1.creation_date_time) as end_date_time,
'C' as business_day_code,
rr.source_system_code,
current_ts AS dw_last_update_date_time

from 
{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('1HR RESCIND OFFER', 'RESCIND OFFER')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event coe --joining to enwisen data
on rr.recruitment_requisition_num_text = coe.recruitment_requisition_num_text
--and s.candidate_sid = coe.candidate_sid
and coe.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type ro
on cast(ro.event_type_id as string) = coe.event_type_id

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ro.event_type_desc)) = 'DRUG SCREEN COMPLETION' 
and coe.completed_date <= cast(coalesce(s1.event_date_time, s1.creation_date_time) as date)
and coe.completed_date > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--background check complete to move to hire
select
s.candidate_profile_sid,
10 as microstep_num,
coalesce(s2.event_date_time, s2.creation_date_time) as start_date_time,
coalesce(s1.event_date_time, s1.creation_date_time) as end_date_time, 
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('MOVE TO HIRE', 'CONFIRM PENDING') --no 'Move to Hire' in ats, so 'Confirm Pending' is closest equivalent
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ( 'BCKGRND CHK COMPLETE', '1HR BACKGROUND CHECK COMPLETE')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s2.event_date_time, s2.creation_date_time)  <= coalesce(s1.event_date_time, s1.creation_date_time)
and cast(coalesce(s2.event_date_time, s2.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--background check complete to pre-adverse
select
s.candidate_profile_sid,
11 as microstep_num,
coalesce(s1.event_date_time, s1.creation_date_time) as start_date_time,
coalesce(s2.event_date_time, s2.creation_date_time) as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ( 'BCKGRND CHK COMPLETE', '1HR BACKGROUND CHECK COMPLETE')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')

	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) = 'HRSM PREADVERSE'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s2.event_date_time, s2.creation_date_time)  <= coalesce(s1.event_date_time, s1.creation_date_time) 
and cast(coalesce(s2.event_date_time, s2.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--background check complete  to decision pending
select
s.candidate_profile_sid,
12 as microstep_num,
coalesce(s1.event_date_time, s1.creation_date_time) as start_date_time,
coalesce(s2.event_date_time, s2.creation_date_time) as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ( 'BCKGRND CHK COMPLETE', '1HR BACKGROUND CHECK COMPLETE')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) = 'DECISION PENDING'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s1.event_date_time, s1.creation_date_time) <= coalesce(s2.event_date_time, s2.creation_date_time) 
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--decision pending to move to hire
select
s.candidate_profile_sid,
13 as microstep_num,
coalesce(s1.event_date_time, s1.creation_date_time) as start_date_time,
coalesce(s2.event_date_time, s2.creation_date_time) as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) = 'DECISION PENDING'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('MOVE TO HIRE', 'CONFIRM PENDING') --no 'Move to Hire' in ats, so 'Confirm Pending' is closest equivalent
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s1.event_date_time, s1.creation_date_time)  <= coalesce(s2.event_date_time, s2.creation_date_time)
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--decision pending to preadverse
select
s.candidate_profile_sid,
14 as microstep_num,
coalesce(s1.event_date_time, s1.creation_date_time) as start_date_time,
coalesce(s2.event_date_time, s2.creation_date_time) as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) = 'DECISION PENDING'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) = 'HRSM PREADVERSE'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s1.event_date_time, s1.creation_date_time) <= coalesce(s2.event_date_time, s2.creation_date_time)
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date)> cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--pre adverse to move to hire
select
s.candidate_profile_sid,
15 as microstep_num,
coalesce(s1.event_date_time, s1.creation_date_time) as start_date_time,
coalesce(s2.event_date_time, s2.creation_date_time) as end_date_time,
'B' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) = 'HRSM PREADVERSE'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('MOVE TO HIRE', 'CONFIRM PENDING')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s1.event_date_time, s1.creation_date_time) <= coalesce(s2.event_date_time, s2.creation_date_time)
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--pre adverse to rescind
select
s.candidate_profile_sid,
16 as microstep_num,
coalesce(s1.event_date_time, s1.creation_date_time) as start_date_time,
coalesce(s2.event_date_time, s2.creation_date_time) as end_date_time,
'B' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) = 'HRSM PREADVERSE'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('1HR RESCIND OFFER', 'RESCIND OFFER')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s1.event_date_time, s1.creation_date_time) <= coalesce(s2.event_date_time, s2.creation_date_time)
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--background check complete to rescind
select
s.candidate_profile_sid,
17 as microstep_num,
coalesce(s1.event_date_time, s1.creation_date_time) as start_date_time,
coalesce(s2.event_date_time, s2.creation_date_time) as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ( 'BCKGRND CHK COMPLETE', '1HR BACKGROUND CHECK COMPLETE')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('1HR RESCIND OFFER', 'RESCIND OFFER')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s1.event_date_time, s1.creation_date_time) <= coalesce(s2.event_date_time, s2.creation_date_time)
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--decision pending to rescind
select
s.candidate_profile_sid,
18 as microstep_num,
coalesce(s1.event_date_time, s1.creation_date_time) as start_date_time,
coalesce(s2.event_date_time, s2.creation_date_time) as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) = 'DECISION PENDING'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('1HR RESCIND OFFER', 'RESCIND OFFER')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s1.event_date_time, s1.creation_date_time) <= coalesce(s2.event_date_time, s2.creation_date_time)
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--move to hire to hire hired
select
s.candidate_profile_sid,
19 as microstep_num,
coalesce(s1.event_date_time, s1.creation_date_time) as start_date_time,
coalesce(s2.event_date_time, s2.creation_date_time) as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('MOVE TO HIRE', 'CONFIRM PENDING') --no 'Move to Hire' in ats, so 'Confirm Pending' is closest equivalent
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('HIRED', 'CONFIRMED COMPLETED') --no 'Hired'' in ats, 'Confirmed Completed' is closest equivalent
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(s1.event_date_time, s1.creation_date_time) <= coalesce(s2.event_date_time, s2.creation_date_time)
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--hired hire to onboarding triggered
select
s.candidate_profile_sid,
20 as microstep_num,
coalesce(cast(s1.event_date_time as date), cast(s1.creation_date_time as date)) as start_date_time, --enwisen only has dates, so casting taleo to match
co.tour_start_date as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from 
{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('HIRED', 'CONFIRMED COMPLETED') --no 'Hired'' in ats, 'Confirmed Completed' is closest equivalent
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

inner join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding co --joining to enwisen
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid
and co.valid_to_date = datetime("9999-12-31 23:59:59")

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) <= co.tour_start_date
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

union all

--hired hire to confirmed
select
s.candidate_profile_sid,
21 as microstep_num,
coalesce(cast(s1.event_date_time as date), cast(s1.creation_date_time as date)) as start_date_time, --enwisen only has dates, so casting taleo to match
co.onboarding_confirmation_date as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from 
{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('HIRED', 'CONFIRMED COMPLETED')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

inner join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding co --joining to enwisen data
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid
and co.valid_to_date = datetime("9999-12-31 23:59:59")

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) <= co.onboarding_confirmation_date
and cast(coalesce(s1.event_date_time, s1.creation_date_time) as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

qualify row_number()over(partition by s.candidate_profile_sid order by co.tour_start_date desc, applicant_num desc)=1 --some applicants can have multiple tour dates, so picking the latest one

union all

--tour start to completion
select
s.candidate_profile_sid,
22 as microstep_num,
co.tour_start_date as start_date_time,
cast(coe.completed_date as date) as end_date_time, 
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from 
{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding co --joining to enwisen
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid
and co.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event coe
on co.recruitment_requisition_num_text = coe.recruitment_requisition_num_text
--and s.candidate_sid = coe.candidate_sid
and coe.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type ro
on cast(ro.event_type_id as string) = coe.event_type_id

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ro.event_type_desc)) = 'TOUR COMPLETION'
and co.tour_start_date <= cast(coe.completed_date as date)
and cast(co.tour_start_date as date) > cast(date_add(cast(current_ts as date), interval -13 month) as date)

qualify row_number()over(partition by s.candidate_profile_sid order by co.tour_start_date desc, applicant_num desc)=1 --some applicants can have multiple tour dates, so picking the latest one

union all

--tour completion to confirmed
select
s.candidate_profile_sid,
23 as microstep_num,
cast(coe.completed_date as date) as start_date_time,
co.onboarding_confirmation_date as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from 
{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding co --joining to enwisen
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid
and co.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event coe
on co.recruitment_requisition_num_text = coe.recruitment_requisition_num_text
--and s.candidate_sid = coe.candidate_sid
and coe.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type ro
on cast(ro.event_type_id as string) = coe.event_type_id

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ro.event_type_desc)) = 'TOUR COMPLETION'
and cast(coe.completed_date as date) <= co.onboarding_confirmation_date
and cast(coe.completed_date as date) > date_add(cast(current_ts as date), interval -13 month)

qualify row_number()over(partition by s.candidate_profile_sid order by co.tour_start_date desc, applicant_num desc)=1 --some applicants can have multiple tour dates, so picking the latest one

union all

--tour completion to confirmed (no preboarding)
select
s.candidate_profile_sid,
24 as microstep_num,
cast(coe.completed_date as date) as start_date_time,
co.onboarding_confirmation_date as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from 
{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding co --joining to enwisen 
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid
and co.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_email_to_hr_status hr
on co.email_sent_status_id = hr.email_sent_status_id

inner join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event coe
on UPPER(TRIM(co.recruitment_requisition_num_text)) = upper(trim(coe.recruitment_requisition_num_text))
--and s.candidate_sid = coe.candidate_sid
and coe.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type ro
on upper(trim(coe.event_type_id)) = upper(trim(cast(ro.event_type_id as string)))

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(hr.hr_status_desc)) = 'VERIFICATION TOUR'
and upper(trim(ro.event_type_desc)) = 'TOUR COMPLETION'
and cast(coe.completed_date as date) <= co.onboarding_confirmation_date
and cast(coe.completed_date as date) > date_add(cast(current_ts as date), interval -13 month)

qualify row_number()over(partition by s.candidate_profile_sid order by co.tour_start_date desc, applicant_num desc)=1 --some applicants can have multiple tour dates, so picking the latest one

union all

--recuriting + hroc end to end
select
s.candidate_profile_sid,
25 as microstep_num,
coalesce(cast(st1.event_date_time as date), cast(st1.creation_date_time as date)) as start_date_time, --enwisen only uses dates so casting taleo to match
coalesce(co.onboarding_confirmation_date, cast(s2.event_date_time as date)) as end_date_time, --pulls either the date onboarding was confirmed or date from taleo it was rescinded
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num asc)=1 --pulls the latest offer
) o
on s.submission_sid = o.submission_sid

left join (

	select
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.creation_date_time,
	rss.submission_status_name

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(rss.submission_status_name)) = 'EXTENDED'
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
)st1

on st1.candidate_profile_sid = s.candidate_profile_sid
and o.extend_date = coalesce(cast(st1.event_date_time as date), cast(st1.creation_date_time as date)) --pulls the offer date from st that matches extend date in offer

left join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding co --joining to enwisen
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid
and co.valid_to_date = datetime("9999-12-31 23:59:59")

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('1HR RESCIND OFFER', 'RESCIND OFFER')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(cast(st1.event_date_time as date), cast(st1.creation_date_time as date)) <= coalesce(co.onboarding_confirmation_date, cast(s2.event_date_time as date))
and coalesce(cast(st1.event_date_time as date), cast(st1.creation_date_time as date)) > date_add(cast(current_ts as date), interval -13 month)

qualify row_number()over(partition by s.candidate_profile_sid order by co.tour_start_date desc, applicant_num desc)=1 --some applicants can have multiple tour dates, so picking the latest one

union all

--hroc only end to end
select
s.candidate_profile_sid,
26 as microstep_num,
coalesce(cast(s1.event_date_time as date), cast(s2.creation_date_time as date)) as start_date_time, --pulling taleo's event_date_time and ats's creation_date_time since ats is always null for event_date_time
coalesce(co.onboarding_confirmation_date, cast(s3.event_date_time as date)) as end_date_time,  --pulls either the date onboarding was confirmed or date from taleo it was rescinded
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2' --moves to hroc
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'T' --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2' --moves to hroc
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'B' --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time asc, st.event_date_time asc, submission_tracking_num asc)=1 --if a submission has the same step multiple times, this pulls the first record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

left join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding co --joining to enwisen
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid
and co.valid_to_date = datetime("9999-12-31 23:59:59")

left join (	
    select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('1HR RESCIND OFFER', 'RESCIND OFFER')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record

)s3

on s.candidate_profile_sid = s3.candidate_profile_sid

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and coalesce(cast(s1.event_date_time as date), cast(s2.creation_date_time as date)) <= coalesce(co.onboarding_confirmation_date, cast(s3.event_date_time as date))
and coalesce(cast(s1.event_date_time as date), cast(s2.creation_date_time as date)) > date_add(cast(current_ts as date), interval -13 month)

qualify row_number()over(partition by s.candidate_profile_sid order by co.tour_start_date desc, applicant_num desc)=1 --if an applicant has multiple tours, this pulls the latest one

union all

--round 2 to background check authorization, unless taleo after 12/8/21 then it's background check authorization to round 2
select
s.candidate_profile_sid,
27 as microstep_num,
case when cast(s3.creation_date_time as date) >= '2021-12-09' and upper(trim(s3.source_system_code)) = 'T' 
then co.completed_date --if offer was extended after 12/8/21 for taleo, use the background auth date from enwisen
when cast(s3.creation_date_time as date) >= '2022-03-10' and upper(trim(s3.source_system_code)) = 'B'
then co.completed_date --for hdm-2076
else coalesce(s1.event_date_time, s2.creation_date_time) --otherwise use the round 2 date
end as start_date_time,
case when cast(s3.creation_date_time as date) >= '2021-12-09' and upper(trim(s3.source_system_code)) = 'T' 
then coalesce(s1.event_date_time, s2.creation_date_time) --if offer was extended after 12/8/21 for taleo, use round 2 as end date
when cast(s3.creation_date_time as date) >= '2022-03-10' and upper(trim(s3.source_system_code)) = 'B'
then coalesce(s1.event_date_time, s2.creation_date_time) --for hdm-2076
else co.completed_date --otherwise use background auth date from enwisen
end as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join (
	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls the latest offer
) o
on s.submission_sid = o.submission_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time,
	st.submission_tracking_num

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'T'  --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'B'  --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time asc, st.event_date_time asc, submission_tracking_num asc)=1 --if a submission has the same step multiple times, this pulls the first record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

left join (
	select
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	rss.submission_status_name,
	st.submission_tracking_num,
	st.creation_date_time,
	st.source_system_code

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(rss.submission_status_name)) = 'EXTENDED'
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
)s3

on s.candidate_profile_sid = s3.candidate_profile_sid
and o.extend_date = coalesce(cast(s3.event_date_time as date), cast(s3.creation_date_time as date)) --pulls date offer was extended because any done after 12/8 for taleo have different logic applied (hdm-2047)

left join (
	select 
	cano.candidate_sid,
	cano.requisition_sid,
	coe.completed_date

	from {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding cano --joining to enwisen data

	inner join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event coe
	on cano.recruitment_requisition_num_text = coe.recruitment_requisition_num_text
	--and s.candidate_sid = coe.candidate_sid
	and coe.valid_to_date = datetime("9999-12-31 23:59:59")

	inner join {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type ro
	on coe.event_type_id = cast(ro.event_type_id as string)

	where cano.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(ro.event_type_desc)) = 'AUTHORIZED BACKGROUND CHECK'
)co
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and s.candidate_profile_sid is not  null
and case when cast(s3.creation_date_time as date) >= '2021-12-09' and upper(trim(s3.source_system_code)) = 'T' 
then co.completed_date --if offer was extended after 12/8/21 for taleo, use the background auth date from enwisen
when cast(s3.creation_date_time as date) >= '2022-03-10' and upper(trim(s3.source_system_code)) = 'B'
then co.completed_date --for hdm-2076
else coalesce(s1.event_date_time, s2.creation_date_time) --otherwise use the round 2 date
end <= case when cast(s3.creation_date_time as date) >= '2021-12-09' and upper(trim(s3.source_system_code)) = 'T' 
then coalesce(s1.event_date_time, s2.creation_date_time) --if offer was extended after 12/8/21 for taleo, use round 2 as end date
when cast(s3.creation_date_time as date) >= '2022-03-10' and upper(trim(s3.source_system_code)) = 'B'
then coalesce(s1.event_date_time, s2.creation_date_time) --for hdm-2076
else co.completed_date --otherwise use background auth date from enwisen
end 
and case when cast(s3.creation_date_time as date) >= '2021-12-09' and upper(trim(s3.source_system_code)) = 'T' 
then cast(co.completed_date as date) --if offer was extended after 12/8/21 for taleo, use the background auth date from enwisen
when cast(s3.creation_date_time as date) >= '2022-03-10' and upper(trim(s3.source_system_code)) = 'B'
then cast(co.completed_date as date) --for hdm-2076
else cast(coalesce(s1.event_date_time, s2.creation_date_time) as date) --otherwise use the round 2 date
end > date_add('2023-06-25', interval -13 month)

qualify row_number()over(partition by s.candidate_profile_sid order by s1.event_date_time desc, s2.creation_date_time asc, co.completed_date desc)=1

union all

--round 2/background check auth to pre employment screen initiated
select
s.candidate_profile_sid,
28 as microstep_num,
case when cast(s4.creation_date_time as date) >= '2021-12-09' and upper(trim(s4.source_system_code)) = 'T' 
then coalesce(s1.event_date_time, s2.creation_date_time)  --if offer was extended after 12/8/21 for taleo, use round 2
when cast(s4.creation_date_time as date) >= '2022-03-10' and upper(trim(s4.source_system_code)) = 'B'
then coalesce(s1.event_date_time, s2.creation_date_time) --for hdm-2076
else co.completed_date --otherwise use the background auth date from enwisen
end as start_date_time,
coalesce(s3.event_date_time, s3.creation_date_time) as end_date_time,
'C' as business_day_code,
s.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition rr

left join {{ params.param_hr_base_views_dataset_name }}.submission s
on rr.recruitment_requisition_sid = s.recruitment_requisition_sid
and s.valid_to_date = datetime("9999-12-31 23:59:59")

left join (
	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls the latest offer
) o
on s.submission_sid = o.submission_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'T'  --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s1

on s.candidate_profile_sid = s1.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st
	
	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_step rsst
	on st.tracking_step_id = rsst.step_id

	where upper(trim(rsst.step_short_name)) = 'ROUND 2'
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(st.source_system_code)) = 'B'  --filtering on ssc because we want the latest taleo record whereas we want the first ats record
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time asc, st.event_date_time asc, submission_tracking_num asc)=1 --if a submission has the same step multiple times, this pulls the first record
)s2

on s.candidate_profile_sid = s2.candidate_profile_sid

left join (
	select 
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	st.tracking_step_id,
	st.creation_date_time

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where upper(trim(rss.submission_status_name)) in ('PRE-EMPLOYMENT SCREEN INITIATED', 'PREEMP SCREEN INIT')
	and st.valid_to_date = datetime("9999-12-31 23:59:59")
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
	
	qualify row_number()over(partition by st.candidate_profile_sid order by st.creation_date_time desc, st.event_date_time desc, submission_tracking_num desc)=1 --if a submission has the same step multiple times, this pulls the latest record
)s3

on s.candidate_profile_sid = s3.candidate_profile_sid

left join (
	select
	st.submission_tracking_sid,
	st.candidate_profile_sid,
	st.event_date_time,
	rss.submission_status_name,
	st.submission_tracking_num,
	st.creation_date_time,
	st.source_system_code

	from {{ params.param_hr_base_views_dataset_name }}.submission_tracking st

	left join {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status sts
	on st.submission_tracking_sid = sts.submission_tracking_sid
	and sts.valid_to_date = datetime("9999-12-31 23:59:59")

	left join {{ params.param_hr_base_views_dataset_name }}.ref_submission_status rss
	on rss.submission_status_id = sts.submission_status_id

	where st.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(rss.submission_status_name)) = 'EXTENDED'
	and (step_reverted_ind is null or upper(trim(step_reverted_ind)) = 'N')
)s4

on s.candidate_profile_sid = s3.candidate_profile_sid
and o.extend_date = coalesce(cast(s4.event_date_time as date), cast(s4.creation_date_time as date)) --pulls date offer was extended because any done after 12/8 for taleo have different logic applied (hdm-2047)

left join (
	select 
	cano.candidate_sid,
	cano.requisition_sid,
	coe.completed_date

	from {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding cano --joining to enwisen data

	inner join {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event coe
	on cano.recruitment_requisition_num_text = coe.recruitment_requisition_num_text
	--and s.candidate_sid = coe.candidate_sid
	and coe.valid_to_date = datetime("9999-12-31 23:59:59")

	inner join {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type ro
	on coe.event_type_id = cast(ro.event_type_id as string)

	where cano.valid_to_date = datetime("9999-12-31 23:59:59")
	and upper(trim(ro.event_type_desc)) = 'AUTHORIZED BACKGROUND CHECK'
)co
on s.candidate_sid = co.candidate_sid
and rr.lawson_requisition_sid = co.requisition_sid

where rr.valid_to_date = datetime("9999-12-31 23:59:59")
and case when cast(s4.creation_date_time as date) >= '2021-12-09' and upper(trim(s4.source_system_code)) = 'T' 
then cast(coalesce(s1.event_date_time, s2.creation_date_time) as date) --if offer was extended after 12/8/21 for taleo, use round 2
when cast(s4.creation_date_time as date) >= '2022-03-10' and upper(trim(s4.source_system_code)) ='B'
then cast(coalesce(s1.event_date_time, s2.creation_date_time) as date) --for hdm-2076
else co.completed_date --otherwise use the background auth date from enwisen
end <= cast(coalesce(s3.event_date_time, s3.creation_date_time) as date)
and case when cast(s4.creation_date_time as date) >= '2021-12-09' and upper(trim(s4.source_system_code)) ='T' 
then cast(coalesce(s1.event_date_time, s2.creation_date_time) as date)  --if offer was extended after 12/8/21 for taleo, use round 2
when cast(s4.creation_date_time as date) >= '2022-03-10' and upper(trim(s4.source_system_code)) ='B'
then cast(coalesce(s1.event_date_time, s2.creation_date_time) as date) --for hdm-2076
else cast(co.completed_date as date) --otherwise use the background auth date from enwisen
end > date_add(cast(current_ts as date), interval -13 month)

qualify row_number()over(partition by s.candidate_profile_sid order by s1.event_date_time desc, s2.creation_date_time asc, co.completed_date desc)=1
);


BEGIN TRANSACTION;

-- delete prior snapshot

delete
from {{ params.param_hr_core_dataset_name }}.candidate_pathway_microstep where 1 = 1;


--assigns each candidate to a pathway and loads only the microsteps for that specific pathway. a candidate can only have one pathway.
insert into {{ params.param_hr_core_dataset_name }}.candidate_pathway_microstep (

candidate_profile_sid,
pathway_id,
microstep_num,
microstep_start_date_time,
microstep_end_date_time,
source_system_code,
dw_last_update_date_time)

--pathway 1 - bg, no adjudication, no ds; accepted to confirmed
select 
candidate_profile_sid,
pathway_id,
microstep_num,
start_date_time,
end_date_time,
source_system_code,
dw_last_update_date_time 

from (
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and adj.adjudication_ind is null and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y' and ds.drug_screen_ind is null and rs.offer_rescind_ind is null and upper(trim(ps.prescreen_ind)) = 'Y'
	then 3
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulling all microsteps the candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 3
) sla --only loading those microsteps that belong to the specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('EXTENDED','ACCEPTED') 
and upper(trim(bc.background_check_ind)) = 'Y' 
and adj.adjudication_ind is null and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y' and ds.drug_screen_ind is null and rs.offer_rescind_ind is null and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 2 - bg, no adjudication, w/ ds; accepted to confirmed
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and adj.adjudication_ind is null and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y' and upper(trim(ds.drug_screen_ind)) = 'Y' and rs.offer_rescind_ind is null and upper(trim(ps.prescreen_ind)) = 'Y'
	then 5
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulling latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulling all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 5
) sla --only pulling the microsteps assocated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('EXTENDED','ACCEPTED') 
and upper(trim(bc.background_check_ind)) = 'Y' and adj.adjudication_ind is null and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y' and upper(trim(ds.drug_screen_ind)) = 'Y' and rs.offer_rescind_ind is null and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 3 - bg, adjudication to proceed, no ds; accepted to confirmed
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y' and ds.drug_screen_ind is null and rs.offer_rescind_ind is null and upper(trim(ps.prescreen_ind)) = 'Y'
	then 4
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select distinct microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 4 --only pulling the microsteps assocated with the candidate's specific pathway
) sla 
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('EXTENDED','ACCEPTED') 
and upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y' and ds.drug_screen_ind is null and rs.offer_rescind_ind is null and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 4 - bg, adjudication to proceed, w/ ds; accepted to confirmed
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y' and upper(trim(ds.drug_screen_ind)) ='Y' and rs.offer_rescind_ind is null and upper(trim(ps.prescreen_ind)) = 'Y'
	then 8
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 8
) sla --only pulling the microsteps assocated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('EXTENDED','ACCEPTED') 
and upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y' and upper(trim(ds.drug_screen_ind)) ='Y' and rs.offer_rescind_ind is null and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 5 - bg, adjudication to dispute to proceed, w/ or w/o ds; accepted to confirmed
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and upper(trim(pa.preadverse_ind)) ='Y' and upper(trim(oc.onboard_confirm_ind)) = 'Y'and rs.offer_rescind_ind is null and upper(trim(ps.prescreen_ind)) = 'Y'
	then 2
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulling latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulling all microsteps a candidate went through 
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select distinct microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 2 --only pulling the microsteps assocated with the candidate's specific pathway
) sla
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = 
 ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('EXTENDED','ACCEPTED') 
and upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and upper(trim(pa.preadverse_ind)) ='Y' and upper(trim(oc.onboard_confirm_ind)) = 'Y'and rs.offer_rescind_ind is null and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 6 - bg, adjudication to dispute to rescind, w/ or w/o ds; accepted to rescind
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'
	then 13
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulling the latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulling all microsteps the candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select distinct microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 13 --only pulling the microsteps assocated with the candidate's specific pathway
) sla
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('ACCEPTED', 'RENEGED','RESCINDED') 
and upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 6a - rescind after pre-adverse process
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and upper(trim(pa.preadverse_ind)) ='Y' and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'
	then 15
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 15
) sla ----only pulling the microsteps assocated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('ACCEPTED', 'RENEGED','RESCINDED') 
and upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and upper(trim(pa.preadverse_ind)) ='Y' and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 6b - rescind due to something other than the background
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'
	then 18
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 18 --only pulling the microsteps assocated with the candidate's specific pathway
) sla
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('ACCEPTED', 'RENEGED','RESCINDED') 
and upper(trim(bc.background_check_ind)) = 'Y' and upper(trim(adj.adjudication_ind)) = 'Y' and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 7 - bg, hea adjudication w/ or w/o ds; accepted to rescind
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and adj.adjudication_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'
	then 10
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 10
) sla --only pulling the microsteps assocated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('RENEGED','RESCINDED') 
and upper(trim(bc.background_check_ind)) = 'Y' and adj.adjudication_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 7a - rescind after hea and pre-adverse process
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and adj.adjudication_ind is null and upper(trim(pa.preadverse_ind)) = 'Y' and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'
	then 14
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 14
) sla ----only pulling the microsteps assocated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('ACCEPTED', 'RENEGED','RESCINDED') 
and upper(trim(bc.background_check_ind)) = 'Y' and adj.adjudication_ind is null and upper(trim(pa.preadverse_ind)) = 'Y' and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 7b - auto-rescind due to ineligibility 
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'
	then 17
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select  microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 17
) sla --only pulling the microsteps assocated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('ACCEPTED', 'RENEGED','RESCINDED') 
and upper(trim(bc.background_check_ind)) = 'Y' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 8 - bg, hea adjudication, w/ or w/o ds, accepted to confirmed
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'Y' and adj.adjudication_ind is null and upper(trim(pa.preadverse_ind)) = 'Y' and upper(trim(oc.onboard_confirm_ind)) = 'Y' and  rs.offer_rescind_ind is null and upper(trim(ps.prescreen_ind)) = 'Y'
	then 6
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 6
) sla --only pulling the microsteps assocated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('EXTENDED', 'ACCEPTED')
and upper(trim(bc.background_check_ind)) = 'Y' and adj.adjudication_ind is null and upper(trim(pa.preadverse_ind)) = 'Y' and upper(trim(oc.onboard_confirm_ind)) = 'Y' and  upper(trim(rs.offer_rescind_ind)) is null and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 9 - no bg, no ds; accepted to confirmed
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y'and ds.drug_screen_ind is null and rs.offer_rescind_ind is null and ps.prescreen_ind is null
	then 7
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulling latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulling all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 7
) sla --only pulling the microsteps associated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('EXTENDED', 'ACCEPTED')
and upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y'and ds.drug_screen_ind is null and rs.offer_rescind_ind is null and ps.prescreen_ind is null

union all

--pathway 10 - no bg, w/ds; accepted to confirmed
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y'and upper(trim(ds.drug_screen_ind)) ='Y' and rs.offer_rescind_ind is null and ps.prescreen_ind is null
	then 9
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --only pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 9
) sla --only pulling the microsteps associated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('EXTENDED', 'ACCEPTED')
and upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and upper(trim(oc.onboard_confirm_ind)) = 'Y'and upper(trim(ds.drug_screen_ind)) ='Y' and rs.offer_rescind_ind is null and ps.prescreen_ind is null

union all

--pathway 11 - no bg, accepted to rescind,  w/ or w/o ds
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) ='Y'
	then 12
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select distinct microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 12
) sla --only pulling the microsteps associated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('RESCINDED', 'RENEGED')
and upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) ='Y'

union all

--pathway 11a - rescind due to drug screen results
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'
	then 16
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 16
) sla --only pulling the microsteps associated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('RESCINDED', 'RENEGED')
and upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and upper(trim(ps.prescreen_ind)) = 'Y'

union all

--pathway 11b - requistion swaps
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and ps.prescreen_ind is null
	then 19
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through 
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select distinct microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 19
) sla --only pulling the microsteps associated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('RESCINDED', 'RENEGED')
and upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and upper(trim(rs.offer_rescind_ind)) = 'Y' and ps.prescreen_ind is null

union all

--pathway 12 - accepted but never confirmed
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and ds.drug_screen_ind is null and rs.offer_rescind_ind is null and ps.prescreen_ind is null
	then 1
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 1
) sla --only pulling the microsteps associated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('EXTENDED', 'ACCEPTED')
and upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and ds.drug_screen_ind is null and rs.offer_rescind_ind is null and ps.prescreen_ind is null

union all

--pathway 13 - dropout without rescind from hroc
select
ms.candidate_profile_sid,
case 
	when upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and ds.drug_screen_ind is null and rs.offer_rescind_ind is null and ps.prescreen_ind is null
	then 11
	else null
end as pathway_id,
ms.microstep_num,
ms.start_date_time,
ms.end_date_time,
ms.source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.submission s

left join (

	select 
	o.submission_sid,
	o.accept_date,
	o.extend_date,
	o.offer_sid

	from
	{{ params.param_hr_base_views_dataset_name }}.offer o

	where valid_to_date = datetime("9999-12-31 23:59:59")
	and o.extend_date is not null
	qualify row_number()over(partition by o.submission_sid order by o.sequence_num desc)=1 --pulls latest offer
) o
on s.submission_sid = o.submission_sid

inner join {{ params.param_hr_base_views_dataset_name }}.offer_status os
on o.offer_sid = os.offer_sid
and os.valid_to_date = datetime("9999-12-31 23:59:59")

inner join {{ params.param_hr_base_views_dataset_name }}.ref_offer_status ros
on os.offer_status_id = ros.offer_status_id

inner join ms --pulls all microsteps a candidate went through
on s.candidate_profile_sid = ms.candidate_profile_sid

inner join (
	select microstep_num
	from {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
	where pathway_id = 11
) sla --only pulling the microsteps associated with the candidate's specific pathway
on ms.microstep_num = sla.microstep_num

left join bc
on bc.candidate_profile_sid = ms.candidate_profile_sid

left join adj
on adj.candidate_profile_sid = ms.candidate_profile_sid

left join pa
on pa.candidate_profile_sid = ms.candidate_profile_sid

left join oc
on oc.candidate_profile_sid = ms.candidate_profile_sid

left join ds
on ds.candidate_profile_sid = ms.candidate_profile_sid

left join rs
on rs.candidate_profile_sid = ms.candidate_profile_sid

left join ps
on ps.candidate_profile_sid = ms.candidate_profile_sid

where s.valid_to_date = datetime("9999-12-31 23:59:59")
and upper(trim(ros.offer_status_desc)) in ('RESCINDED', 'RENEGED')
and upper(trim(bc.background_check_ind)) = 'N' and adj.adjudication_ind is null and pa.preadverse_ind is null and oc.onboard_confirm_ind is null and ds.drug_screen_ind is null and rs.offer_rescind_ind is null and ps.prescreen_ind is null
)a
qualify row_number() over (partition by candidate_profile_sid, pathway_id, microstep_num order by start_date_time desc, end_date_time desc )=1;

COMMIT TRANSACTION;

drop table bc;
drop table adj;
drop table pa;
drop table oc;
drop table ds;
drop table rs;
drop table ps;
drop table ms;
 
end;