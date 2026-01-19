export JOBNAME='J_CN_PATIENT_STAGING_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_STAGING_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(Select ABC.PatientClinicalStagingFactID as CN_Patient_Staging_SID
,PatientDimID as Nav_Patient_Id
,TumorTypeDimID as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
--,FacilityDimID as FacilityDimID
,Coid as Coid
,'H' as Company_Code
,'C' as Cancer_Stage_Classification_Method_Code
--,GeneralDiagnosisName as GeneralDiagnosisName
,ABC.Cancer_Staging_Result_Code as Cancer_Staging_Result_Code
,ABC.Cancer_Staging_Type_Code as Cancer_Staging_Type_Code 
,ClinicalStage as Cancer_Stage_Code
,concat('0x',CONVERT(varchar(50),PCS.HBSource,2)) as Hashbite_SSK
--,PCS.HBSource as Hashbite_SSK
,'N' as Source_System_Code
,current_timestamp as DW_Last_Update_Date_Time

 From Navadhoc.dbo.PatientClinicalStaging (NOLOCK) PCS
 INNER JOIN (
 Select ClinicalCancerCodeT as Cancer_Staging_Result_Code,PatientClinicalStagingFactID,'T' as Cancer_Staging_Type_Code,HBSource   from Navadhoc.dbo.PatientClinicalStaging where ClinicalCancerCodeT is not null 
union
Select ClinicalCancerCodeM as Cancer_Staging_Result_Code,PatientClinicalStagingFactID,'M' as Cancer_Staging_Type_Code,HBSource   from Navadhoc.dbo.PatientClinicalStaging where ClinicalCancerCodeM is not null 
UNION
Select ClinicalCancerCodeN as Cancer_Staging_Result_Code,PatientClinicalStagingFactID,'N' as Cancer_Staging_Type_Code,HBSource  from Navadhoc.dbo.PatientClinicalStaging where ClinicalCancerCodeN is not null  
UNION
Select NULL as Cancer_Staging_Result_Code,PatientClinicalStagingFactID,NULL as Cancer_Staging_Type_Code,HBSource  
from Navadhoc.dbo.PatientClinicalStaging
where clinicalcancercodeN is null and clinicalcancercodet is null and clinicalcancercodem is null
  )ABC
ON ABC.PatientClinicalStagingFactID =PCS.PatientClinicalStagingFactID
AND ABC.HBSource =PCS.HBSource 

 Union 

 Select PPS.PatientPathologicalStagingFactID as CN_Patient_Staging_SID
,PatientDimID as Nav_Patient_Id
,TumorTypeDimID as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
--,FacilityDimID as FacilityDimID
,Coid as Coid
,'H' as Company_Code
,'P' as Cancer_Stage_Classification_Method_Code
--,GeneralDiagnosisName as GeneralDiagnosisName
,patstg.Cancer_Staging_Result_Code as Cancer_Staging_Result_Code
,patstg.Cancer_Staging_Type_Code as Cancer_Staging_Type_Code 
,PathologicalStage as Cancer_Stage_Code
,concat('0x',CONVERT(varchar(50),PPS.HBSource,2)) as Hashbite_SSK
--,PPS.HBSource as Hashbite_SSK
,'N' as Source_System_Code
,current_timestamp as DW_Last_Update_Date_Time

  From Navadhoc.dbo.PatientPathologicalStaging (NOLOCK) PPS 
  INNER JOIN (
 Select PathologicalCancerCodeT as Cancer_Staging_Result_Code,PatientPathologicalStagingFactID,'T' as Cancer_Staging_Type_Code,HBSource   from Navadhoc.dbo.PatientPathologicalStaging where PathologicalCancerCodeT is not null 
union
Select PathologicalCancerCodeM as Cancer_Staging_Result_Code,PatientPathologicalStagingFactID,'M' as Cancer_Staging_Type_Code,HBSource   from Navadhoc.dbo.PatientPathologicalStaging where PathologicalCancerCodeM is not null 
UNION
Select PathologicalCancerCodeN as Cancer_Staging_Result_Code,PatientPathologicalStagingFactID,'N' as Cancer_Staging_Type_Code,HBSource  from Navadhoc.dbo.PatientPathologicalStaging where PathologicalCancerCodeN is not null 
UNION
Select NULL as Cancer_Staging_Result_Code,PatientPathologicalStagingFactID,NULL as Cancer_Staging_Type_Code,HBSource  
from Navadhoc.dbo.PatientPathologicalStaging 
where pathologicalcancercoden is null and pathologicalcancercodem is null and pathologicalcancercodet is null
)patstg
ON patstg.PatientPathologicalStagingFactID= PPS.PatientPathologicalStagingFactID
AND patstg.HBSource =PPS.HBSource )ab"
export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_STAGING_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_PATIENT_STAGING_STG"
