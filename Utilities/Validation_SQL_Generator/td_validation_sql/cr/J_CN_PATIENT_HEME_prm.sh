
export JOBNAME='J_CN_PATIENT_HEME'

export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
(select 
STG.PatientHemeFactID as PatientHemeFactID,
STG.PatientDimID as PatientDimID,
STG.TumorTypeDimID as TumorTypeDimID,
STG.DiagnosisResultID as DiagnosisResultID,
STG.DiagnosisDimID as DiagnosisDimID,
STG.COID as COID,
'H' Company_code,
STG.NavigatorDimID as NavigatorDimID,
STG.Transportation as Transportation,
STG.DrugUseHistory as DrugUseHistory,
DTL.Physician_Id as Physician_Id,
STG.HBSource as HBSource,
'N' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from EDWCR_STaging.CN_Patient_Heme_STG STG

LEFT JOIN EDWCR.CN_Physician_Detail DTL
ON DTL.Physician_Name = STG.Hematologist

where STG.HBSource NOT IN (select TGT.Hashbite_SSK from EDWCR.CN_Patient_Heme TGT)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
qualify row_number() over (partition by PatientHemeFactID order by DTL.Physician_Id desc)=1
) a "

export AC_ACT_SQL_STATEMENT="select '$JOBNAME'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.CN_Patient_Heme
where 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_SCHEMA}.ETL_JOB_RUN where Job_Name = '$JOBNAME')
"


