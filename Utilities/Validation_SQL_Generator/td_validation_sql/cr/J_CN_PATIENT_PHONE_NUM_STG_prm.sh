export JOBNAME='J_CN_PATIENT_PHONE_NUM_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_PHONE_NUM_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(
Select 
PatientDimID as Nav_Patient_Id
 ,'H' as  Phone_Num_Type_Code 
 ,PatientPhone as Phone_Num 
 ,'N' as Source_System_Code
 ,current_timestamp as DW_Last_Update_Date_Time
 from Navadhoc.dbo.DimPatient (NOLOCK) where PatientPhone is not null
 union
 Select 
 PatientDimID as Nav_Patient_Id
 ,'C' as  Phone_Num_Type_Code  
 ,PatientCellPhone as Phone_Num  
 ,'N' as Source_System_Code
 ,current_timestamp as DW_Last_Update_Date_Time
From Navadhoc.dbo.DimPatient (NOLOCK) where PatientCellPhone is not null
)abc
"
export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_PHONE_NUM_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Patient_Phone_Num_STG"
