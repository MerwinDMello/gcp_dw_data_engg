export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_SYSTEM_PRODUCTIVITY_LOG_STG'


export AC_EXP_SQL_STATEMENT="Select 'J_CR_PATIENT_TUMOR_PATHOLOGY_RESULT_STG' + ','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING
from
(
Select SystemProductivityLogID,
FacilityID,
PatientID,
TumorID,
UserID,
StatusDate
From MRegistry.dbo.SystemProductivityLog
) Src"

export AC_ACT_SQL_STATEMENT="Select 'J_CR_SYSTEM_PRODUCTIVITY_LOG_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from 
${NCR_STG_SCHEMA}.CR_System_Productivity_Log_Stg"

