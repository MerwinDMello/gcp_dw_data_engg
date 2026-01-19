export JOBNAME='J_CN_PATIENT_MEDICAL_ONCOLOGY_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_MEDICAL_ONCOLOGY_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(
SELECT 
MedicalOncologyFactID AS CN_Patient_Medical_Oncology_ID
,CoreRecordID AS Core_Record_Type_Id
,MedicalSpecialistDimID AS Med_Spcl_Physician_Id
,PatientDimID AS Nav_Patient_Id
,TumorTypeDimID AS Tumor_Type_Id
,DiagnosisResultID AS Diagnosis_Result_Id
,DiagnosisDimID AS Nav_Diagnosis_Id
,NavigatorDimID AS Navigator_Id
,Coid AS COID
,'H' as Company_Code
,TreatmentTypeID AS Treatment_Type_Id
,MOCoreRecordDate AS Core_Record_Date
,MOActualStartDate AS Treatment_Start_Date
,MOActualEndDate AS Treatment_End_Date
,MOEstimatedEndDate AS Estimated_End_Date
,MOFacility AS Medical_Oncology_Facility_Id
,MODrug AS Drug_Name
,CASE WHEN MODoseDenseChemo ='Dose Dense Chemo' THEN 'Y' ELSE 'N' END Dose_Dense_Chemo_Ind
,MODose AS Drug_Dose_Amt_Text
,MOMeasurement AS Drug_Dose_Measurement_Text
,CASE WHEN UPPER(ltrim(rtrim(MODosingAvailable))) ='YES' then 'Y' 
WHEN UPPER(ltrim(rtrim(MODosingAvailable))) = 'NO' then  'N' else 'U' END AS Drug_Available_Ind
,MOQuantity AS Drug_Qty
,MOCycles AS Cycle_Num
,MOTimesEvery AS Cycle_Frequency_Text
,MOReason AS Medical_Oncology_Reason_Text
,MOTerminated AS Terminated_Ind
,MONeoVsAdj AS Treatment_Therapy_Schedule_Cd
,MOComments AS Comment_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) AS HBSource
FROM NavAdhoc.dbo.PatientMedicalOncology (NOLOCK)

)ab "

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_MEDICAL_ONCOLOGY_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from ${NCR_STG_SCHEMA}.CN_PATIENT_MEDICAL_ONCOLOGY_STG"


