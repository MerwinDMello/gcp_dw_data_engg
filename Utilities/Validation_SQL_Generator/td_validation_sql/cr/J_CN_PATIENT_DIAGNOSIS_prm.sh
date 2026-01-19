export JOBNAME='J_CN_PATIENT_DIAGNOSIS'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_DIAGNOSIS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 
(
Select 
STG.CN_Patient_Diagnosis_SID,
STG.Nav_Patient_Id,
STG.Tumor_Type_Id,
STG.Diagnosis_Result_Id,
STG.Nav_Diagnosis_Id,
STG.Navigator_Id,
STG.Coid,
'H' as Company_Code,
STG.General_Diagnosis_Name,
STG.Diagnosis_Date,
RDD.Diagnosis_Detail_Id,
RS.Side_ID as Diagnosis_Side_Id,
STG.Hashbite_SSK,
'N' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from 
$NCR_STG_SCHEMA.CN_Patient_Diagnosis_STG STG
Left Outer Join $NCR_TGT_SCHEMA.Ref_Side RS
on
STG.DiagnosisSide=RS.Side_Desc

Left Outer Join $NCR_TGT_SCHEMA.Ref_Diagnosis_Detail RDD
on
COALESCE(TRIM(STG.DiagnosisMetastatic),'X')=COALESCE(TRIM(RDD.Diagnosis_Detail_Desc),'X') and
COALESCE(TRIM(STG.DiagnosisIndicator),'XX')=COALESCE(TRIM(RDD.Diagnosis_Indicator_Text),'XX')

where STG.Hashbite_SSK not in ( Select Hashbite_SSK from $NCR_TGT_SCHEMA.CN_Patient_Diagnosis)) SRC"


export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_DIAGNOSIS'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_TGT_SCHEMA}.CN_Patient_Diagnosis
where DW_LAST_UPDATE_DATE_TIME >= (Select max(Job_Start_Date_time) from $NCR_AC_VIEW.ETL_JOB_RUN where Job_Name='J_CN_PATIENT_DIAGNOSIS');"