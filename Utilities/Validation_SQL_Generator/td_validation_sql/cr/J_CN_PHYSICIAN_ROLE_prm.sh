export JOBNAME='J_CN_PHYSICIAN_ROLE'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PHYSICIAN_ROLE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from
(

Select 
Physician_Id
,Physician_Role_Code
from $NCR_STG_SCHEMA.CN_Physician_Role_Stg 

Union	

Select 	
Physician_Id	,
'Gyn'	
From $NCR_TGT_SCHEMA.CN_Physician_Detail 	
inner join $NCR_TGT_SCHEMA.CN_patient	
On Physician_Id=Gynecologist_Physician_Id	

Union	

Select 	
Physician_Id,	
'PCP'	
From $NCR_TGT_SCHEMA.CN_Physician_Detail 	
inner join $NCR_TGT_SCHEMA.CN_patient	
On Physician_Id=Primary_Care_Physician_Id	

Union	

Select 	
Physician_Id,	
'ETP'	
From $NCR_TGT_SCHEMA.CN_Physician_Detail 	
inner join $NCR_TGT_SCHEMA.CN_Patient_Tumor 
On Physician_Id=Treatment_End_Physician_Id

)ab where (Physician_Id,Physician_Role_Code) not in ( Select Physician_Id,Physician_Role_Code from $NCR_TGT_SCHEMA.CN_PHYSICIAN_ROLE ) 
"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PHYSICIAN_ROLE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_TGT_SCHEMA}.CN_PHYSICIAN_ROLE 
where DW_LAST_UPDATE_DATE_TIME >= (Select max(Job_Start_Date_time) from $NCR_AC_VIEW.ETL_JOB_RUN where Job_Name='J_CN_PHYSICIAN_ROLE');"
