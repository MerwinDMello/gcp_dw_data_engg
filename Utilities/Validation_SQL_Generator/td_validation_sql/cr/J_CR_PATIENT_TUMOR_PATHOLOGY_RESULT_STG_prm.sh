export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_PATIENT_TUMOR_PATHOLOGY_RESULT_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CR_PATIENT_TUMOR_PATHOLOGY_RESULT_STG' + ','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING
from
(
Select 
Tumor.PatientId as PatientId,
Tumor.TumorId as TumorId,
TumorExt1a.NodesExamined as NodesExamined,
TumorExt1a.NodesPositive as NodesPositive
from mRegistry.dbo.Tumor Tumor
Left Outer Join mRegistry.dbo.TumorExt1a TumorExt1a
On
Tumor.TumorId=TumorExt1a.TumorId
) Src
"
export AC_ACT_SQL_STATEMENT="Select 'J_CR_PATIENT_TUMOR_PATHOLOGY_RESULT_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CR_Pat_Tumor_Pathology_Result_STG"
