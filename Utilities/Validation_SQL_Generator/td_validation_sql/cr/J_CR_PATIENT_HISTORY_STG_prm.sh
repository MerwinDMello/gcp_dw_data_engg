export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_HISTORY_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CR_PATIENT_HISTORY_STG' + ','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(
Select 
Tumor.PatientId as PatientId,
Tumor.TumorId as TumorId,
TumorExt13.SmokHist as SmokHist,
TumorExt13.AmountTobacco as AmountTobacco,
TumorExt13.YrsTobacco1 as YrsTobacco1,
TumorExt13.FamHxCA as FamHxCA,
TumorExt13.PtHxCA as PtHxCA
from mRegistry.dbo.Tumor Tumor
Left Outer Join mRegistry.dbo.TumorExt13 TumorExt13
On
Tumor.TumorId=TumorExt13.TumorId
) src
"
export AC_ACT_SQL_STATEMENT="Select 'J_CR_PATIENT_HISTORY_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CR_PATIENT_HISTORY_STG"