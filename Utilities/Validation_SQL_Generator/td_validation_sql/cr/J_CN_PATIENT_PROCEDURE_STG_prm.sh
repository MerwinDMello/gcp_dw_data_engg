export JOBNAME='J_CN_PATIENT_PROCEDURE_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_PROCEDURE_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(
SELECT 
PatientProcedureFactID AS CN_Patient_Procedure_SID
,CoreRecordID AS Core_Record_Type_Id
,MedicalSpecialistDimId AS Med_Spcl_Physician_Id
,PatientDimID AS Nav_Patient_Id
,TumorTypeDimId AS Tumor_Type_Id
,DiagnosisResultId AS Diagnosis_Result_Id
,DiagnosisDimID AS Nav_Diagnosis_Id
,Coid AS Coid
,'H' AS Company_Code
,NavigatorDimId AS Navigator_Id
,ProcedureDate AS Procedure_Date
,ProcedureType AS ProcedureType
,CASE WHEN LTRIM(RTRIM(OtherProcedureType)) != '' THEN LTRIM(RTRIM(OtherProcedureType)) ELSE NULL END AS OtherProcedureType
,CASE WHEN LTRIM(RTRIM(OtherSurgeryType)) != '' THEN LTRIM(RTRIM(OtherSurgeryType)) ELSE NULL END AS OtherSurgeryType
,CASE WHEN LTRIM(RTRIM(LinePlacementType)) != '' THEN LTRIM(RTRIM(LinePlacementType)) ELSE NULL END AS LinePlacementType
,CASE WHEN ProcedurePalliative ='Palliative' THEN 'Y' ELSE 'N' END AS Palliative_Ind
,concat('0x',CONVERT(varchar(50),HBSource,2)) AS Hashbite_SSK
FROM NavAdhoc.dbo.PatientProcedure
 )ab"


export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_PROCEDURE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Patient_Procedure_Stg"
