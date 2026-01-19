export JOBNAME='J_CN_PATIENT_TEST_RESULT_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_TEST_RESULT_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(
Select 
PatientTestFactID,
PatientDimID,
TumorTypeDimID,
DiagnosisResultID,
DiagnosisDimID,
NavigatorDimID,
FacilityDimID,
Coid,
'H' Company_Code,
TestDate,
TestName,
CASE 
	WHEN ltrim(rtrim(TestName)) = 'KarnofskyPerformed' THEN 82 
	WHEN ltrim(rtrim(TestName)) = 'ECOGPerformed' THEN 83
    WHEN ltrim(rtrim(TestName)) = 'FEV1Performed' THEN 84
	WHEN ltrim(rtrim(TestName)) = 'DLCOPerformed' THEN 85
END as Test_Type_ID,
Result,
CASE WHEN UPPER(ltrim(rtrim(Result))) = 'PERFORMED' THEN 'Y' ELSE 'N' END as Test_Perform_Ind,
CASE 
	WHEN ltrim(rtrim(TestName)) = 'KarnofskyPerformed' THEN KarnofskyScalePercent 
	WHEN ltrim(rtrim(TestName)) = 'ECOGPerformed' THEN ECOG
    WHEN ltrim(rtrim(TestName)) = 'FEV1Performed' THEN FEV1Pct
	WHEN ltrim(rtrim(TestName)) = 'DLCOPerformed' THEN DLCOPct
END as Test_Val_Num,
concat('0x',CONVERT(varchar(50),HBSource,2)) as HBSource,
'N' as Source_System_Code
from Navadhoc.dbo.PatientTest
 cross apply 
 (
   values 
   ('KarnofskyPerformed',KarnofskyPerformed),
   ('ECOGPerformed',ECOGPerformed),
   ('FEV1Performed',FEV1Performed),
   ('DLCOPerformed',DLCOPerformed)
) c (TestName,Result)
)abc"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_TEST_RESULT_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Patient_TEST_RESULT_STG;"
