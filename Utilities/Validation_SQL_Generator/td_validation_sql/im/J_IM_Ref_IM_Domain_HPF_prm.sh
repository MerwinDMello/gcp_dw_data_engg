export Job_Name='J_IM_Ref_IM_Domain_HPF'
export JOBNAME='J_IM_Ref_IM_Domain_HPF'

export col_name='IM_Domain_Id'
export tab_name='EDWIM.Ref_IM_Domain'

export AC_EXP_SQL_STATEMENT=" select 'J_IM_Ref_IM_Domain_HPF' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
select  count(distinct Instance_Connection_String) as counts from  EDWDW_BASE_VIEWS.Document_Work_Flow_Instance) a" 

export AC_ACT_SQL_STATEMENT="
select 'J_IM_Ref_IM_Domain_HPF' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
 SELECT COUNT(*) as counts FROM
 EDWIM.Ref_IM_Domain where
application_system_id=3)A"