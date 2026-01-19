export JOBNAME='J_CN_PATIENT_HEME_RISK_FACTOR_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_HEME_RISK_FACTOR_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING 
From
(
select 
PatientHemeDiagnosisFactID,
PatientDimID,
TumorTypeDimID,
DiagnosisResultID,
DiagnosisDimID,
FacilityDimID,
COID,
NavigatorDimID,
RiskFactor,
OtherRiskFactor,
TumorDiseaseSite,
OtherTumorDiseaseSite,
CAST(concat('0x',CONVERT(varchar(50),[HBSource],2)) as VARCHAR(60)) as HBSource
from navadhoc.[dbo].[PatientHemeRiskFactor]

) A"
export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_HEME_RISK_FACTOR_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.PATIENT_HEME_RISK_FACTOR_STG"
