update {{params.param_hr_stage_dataset_name}}.ref_pat_sat_que_dom_xwalk_stg
set	domain_id = domain_id,			
    domain_label = nullif(ltrim(domain_label),''),			
    question_id = question_id,			
    domaingroupid = domaingroupid,			
    domaingroupdesc = nullif(ltrim(domaingroupdesc),''),			
    rpt_label = Null,			
    dw_last_update_date_time = CURRENT_DATETIME('US/Central')
where dw_last_update_date_time is null;