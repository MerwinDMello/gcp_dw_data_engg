export JOBNAME='J_CN_PATIENT_CORE_ADHERENCE_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_CORE_ADHERENCE_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(
Select  
PatientCoreAdherenceFactID as CN_Patient_Core_Adherence_SID ,
PatientDimID as Nav_Patient_Id,
TumorTypeDimID as Tumor_Type_Id,
DiagnosisResultID as Diagnosis_Result_Id,
DiagnosisDimID as Nav_Diagnosis_Id,
NavigatorDimID as Navigator_Id,
CASE 
	WHEN ltrim(rtrim(MeasureValue)) = 'CoreImaging' THEN 50 
	WHEN ltrim(rtrim(MeasureValue)) = 'CoreBiopsy' THEN 51
    WHEN ltrim(rtrim(MeasureValue)) = 'CoreSurgery' THEN 52
	WHEN ltrim(rtrim(MeasureValue)) = 'CoreMedicalOncology' THEN 53
	WHEN ltrim(rtrim(MeasureValue)) = 'CoreRadiationOncology' THEN 54
	WHEN ltrim(rtrim(MeasureValue)) = 'CoreProcedures' THEN 55
	WHEN ltrim(rtrim(MeasureValue)) = 'CoreComplications' THEN 56
	WHEN ltrim(rtrim(MeasureValue)) = 'CoreLabs' THEN 57
	WHEN ltrim(rtrim(MeasureValue)) = 'CoreAssessments' THEN 58
	WHEN ltrim(rtrim(MeasureValue)) = 'CoreClinicalTrials' THEN 59 
	WHEN ltrim(rtrim(MeasureValue)) = 'CoreMDM' THEN 60
	
END as Core_Adherence_Measure_Id,
Cast(Result as Varchar(255)) as Core_Adherence_Measure_Text,
Coid,
concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from Navadhoc.dbo.PatientCoreAdherence
Cross Apply 
(
Values 
('CoreImaging',CoreImaging),
   ('CoreBiopsy',CoreBiopsy),
   ('CoreSurgery',CoreSurgery),
   ('CoreMedicalOncology',CoreMedicalOncology),
   ('CoreRadiationOncology',CoreRadiationOncology),
   ('CoreProcedures',CoreProcedures),
   ('CoreComplications',CoreComplications),
   ('CoreLabs',CoreLabs),
   ('CoreAssessments',CoreAssessments),
   ('CoreClinicalTrials',CoreClinicalTrials),
   ('CoreMDM',CoreMDM)
) c (MeasureValue,Result)
Where Result IS NOT NULL AND ltrim(rtrim(Result)) != ''
)ab"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_CORE_ADHERENCE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_PATIENT_CORE_ADHERENCE_STG;"
