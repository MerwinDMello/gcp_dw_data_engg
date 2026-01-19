export Job_Name='J_IM_Ref_IM_Domain_PK'
export JOBNAME=''J_IM_Ref_IM_Domain_PK''
export col_name='IM_Domain_Id'
export tab_name='EDWIM.Ref_IM_Domain'


export AC_EXP_SQL_STATEMENT=" select 'J_IM_Ref_IM_Domain_PK' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
select  count(distinct PK_Database_Instance_Code) as counts from  EDWIM_BASE_VIEWS.Ref_PK_Data_Base_Instance) a" 

export AC_ACT_SQL_STATEMENT="
select 'J_IM_Ref_IM_Domain_PK' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
 SELECT COUNT(*) as counts FROM
 EDWIM.Ref_IM_Domain where
application_system_id=8)A"