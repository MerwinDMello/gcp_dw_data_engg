export JOBNAME='J_REF_DIAGNOSIS_DETAIL_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_REF_DIAGNOSIS_DETAIL_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING 
From
(
SELECT distinct DiagnosisMetastatic, DiagnosisIndicator
FROM Navadhoc.dbo.PatientDiagnosis (NOLOCK)
where DiagnosisMetastatic is not null 
OR DiagnosisIndicator is not null) A"


export AC_ACT_SQL_STATEMENT="Select 'J_REF_DIAGNOSIS_DETAIL_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_DIAGNOSIS_DETAIL_STG"
