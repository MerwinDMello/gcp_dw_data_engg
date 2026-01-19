export Job_Name='J_IM_MT_REF_IM_DOMAIN'
export JOBNAME='J_IM_MT_REF_IM_DOMAIN'

export col_name='IM_Domain_Id'
export tab_name='EDWIM.Ref_IM_Domain'

export AC_EXP_SQL_STATEMENT=" select 'J_IM_MT_REF_IM_DOMAIN' ||','||cast(zeroifnull(A.counts) as varchar
(20)) ||',' as source_string from 
(

sel count(*) as counts from 
(sel count(*) as counts from EDWIM_Staging.stg_ref_im_domain group by 
 Application_system_id,Im_Domain_Name, IM_Domain_Desc, Source_System_Code,Dw_Last_Update_Date_Time
 )B
 ) A " 

export AC_ACT_SQL_STATEMENT="
select 'J_IM_MT_REF_IM_DOMAIN' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
 sel count(*) as counts from  edwim.ref_im_domain
 where cast (Dw_Last_Update_Date_Time as date) = current_date
 and Application_System_Id in (5,6)

)A"