##########################
## Variable Declaration ##
##########################



export JOBNAME='J_HDW_TMS_Ref_Probability_Potential'

export AC_EXP_SQL_STATEMENT="select 'J_HDW_TMS_Ref_Probability_Potential' ||','|| Coalesce(cast(count(*) as varchar(20)), 0)  ||','as SOURCE_STRING 
FROM 


(SELECT 
	Probability_Potential_Desc
FROM ${NCR_STG_SCHEMA}.Ref_Probability_Potential_WRK STG
where  STG.Probability_Potential_Desc not in (sel Probability_Potential_Desc from EDWHR.Ref_Probability_Potential)
) SRC"




export AC_ACT_SQL_STATEMENT="select '$Job_Name'||','||
cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Ref_Probability_Potential 
WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_VIEW}.ETL_JOB_RUN where Job_Name ='J_HDW_TMS_Ref_Probability_Potential')"

