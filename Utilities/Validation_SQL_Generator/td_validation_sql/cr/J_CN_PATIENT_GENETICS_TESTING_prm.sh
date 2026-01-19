export JOBNAME='J_CN_PATIENT_GENETICS_TESTING'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_GENETICS_TESTING'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 

(
Select 
GeneticsTestingFactID,
CoreRecordID,
PatientDimID,
TumorTypeDimId,
DiagnosisResultId,
DiagnosisDimID,
Coid,
'H' as Company_Code,
NavigatorDimId,
GeneticsDate,
GeneticsTestType,
GeneticsSpecialist,
Breast_Cancer_Type_ID,
GeneticsComments,
HBSource,
'N' Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from
$NCR_STG_SCHEMA.CN_Patient_Genetics_Testing_STG PGT
left outer join $NCR_TGT_SCHEMA.Ref_Breast_Cancer_Type BRCA
on 
 PGT.GeneticsBRCAType=BRCA. Breast_Cancer_Type_Desc
 where HBSource not in ( Select Hashbite_SSK from $NCR_TGT_SCHEMA.CN_Patient_Genetics_Testing )
) SRC"



export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_GENETICS_TESTING'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from $NCR_TGT_SCHEMA.CN_Patient_Genetics_Testing
where DW_LAST_UPDATE_DATE_TIME >= (Select max(Job_Start_Date_time) from $NCR_AC_VIEW.ETL_JOB_RUN where Job_Name='J_CN_PATIENT_GENETICS_TESTING');"
