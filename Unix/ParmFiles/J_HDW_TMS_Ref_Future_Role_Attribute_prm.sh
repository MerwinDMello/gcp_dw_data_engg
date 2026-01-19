##########################
## Variable Declaration ##
##########################


export JOBNAME='J_HDW_TMS_Ref_Future_Role_Attribute'

export AC_EXP_SQL_STATEMENT="select 'J_HDW_TMS_Ref_Future_Role_Attribute ' ||','|| Coalesce(cast(count(*) as varchar(20)), 0)  ||','as SOURCE_STRING 
FROM 


(SELECT 
	Future_Role_Attribute_Desc 
FROM ${NCR_STG_SCHEMA}.Ref_Future_Role_Attribute_WRK STG
where  STG.Future_Role_Attribute_Desc
not in (sel Future_Role_Attribute_Desc from EDWHR.Ref_Future_Role_Attribute)
) SRC"

export AC_ACT_SQL_STATEMENT="select 'J_HDW_TMS_Ref_Future_Role_Attribute' ||','|| Coalesce(cast(count(*) as varchar(20)), 0)  ||','as SOURCE_STRING
FROM ${NCR_TGT_SCHEMA}.Ref_Future_Role_Attribute
WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_VIEW}.ETL_JOB_RUN where Job_Name = 'J_HDW_TMS_Ref_Future_Role_Attribute ')"

