export JOBNAME='J_CN_PATIENT_RADIATION_ONCOLOGY_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_RADIATION_ONCOLOGY_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(
SELECT 
PatientRadiationOncologyFactID AS CN_Patient_Rad_Oncology_SID
,CoreRecordID AS Core_Record_Type_Id
,PatientDimID AS Nav_Patient_Id
,TumorTypeDimId AS Tumor_Type_Id
,DiagnosisResultId AS Diagnosis_Result_Id
,DiagnosisDimID AS Nav_Diagnosis_Id
,TreatmentTypeId AS Treatment_Type_Id
,Coid AS Coid
,'H' as Company_Code
,NavigatorDimId AS Navigator_Id
,MedicalSpecialistDimId AS Med_Spcl_Physician_Id
,ROCoreRecordDate AS Core_Record_Date
,ROActualStartDate AS Treatment_Start_Date
,ROEndTreatmentDate AS Treatment_End_Date
,NULL as Treatment_Fractions_Num
,ROLocation AS Treatment_Site_Location_Id
,ROLobes AS Lung_Lobe_Location_Id
,CASE When ROElapse = 'Elapse' then 'Y' else null END AS Elapse_Ind
,ROElapseStart AS Elapse_Start_Date
,ROElapseEnd AS Elapse_End_Date
,ROReason AS Radiation_Oncology_Reason_Text
,CASE When RONeoVsAdj='Neo-adjuvant' then 'Neo' when RONeoVsAdj='Adjuvant' then 'Adj' else null END AS Treatment_Therapy_Schedule_Code
,CASE When ROPalliativeOnly='Palliative Only' then 'Y' else 'N' END AS Palliative_Ind
,ROFacility AS Radiation_Oncology_Facility_Id
,ROComment AS Comment_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) AS Hashbite_SSK
,'N' AS Source_System_Code
FROM NavAdhoc.dbo.PatientRadiationOncology (NOLOCK)

)ab "

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_RADIATION_ONCOLOGY_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from ${NCR_STG_SCHEMA}.CN_PATIENT_RADIATION_ONCOLOGY_STG"


