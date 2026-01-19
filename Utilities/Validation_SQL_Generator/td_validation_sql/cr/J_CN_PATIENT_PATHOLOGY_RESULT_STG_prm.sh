export JOBNAME='J_CN_PATIENT_PATHOLOGY_RESULT_STG'

export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_PATHOLOGY_RESULT_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from 
(
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,1 as Pathology_Result_Type_Id
,KRAS  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where ltrim(rtrim(KRAS)) <>'' AND KRAS is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,2 as Pathology_Result_Type_Id
,BRAF  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where ltrim(rtrim(BRAF)) <>'' AND BRAF is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,3 as Pathology_Result_Type_Id
,MSI  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where ltrim(rtrim(MSI)) <>'' AND MSI is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,4 as Pathology_Result_Type_Id
,Ki67Results  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where ltrim(rtrim(Ki67Results)) <>'' AND Ki67Results is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,5 as Pathology_Result_Type_Id
,Ki67  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where ltrim(rtrim(Ki67)) <>'' AND Ki67 is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,6 as Pathology_Result_Type_Id
,CMET  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where ltrim(rtrim(CMET)) <>'' AND CMET is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,7 as Pathology_Result_Type_Id
,SignetRing  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where ltrim(rtrim(SignetRing)) <>'' AND SignetRing is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,8 as Pathology_Result_Type_Id
,LinitisPlastica  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(LinitisPlastica)) <>'' AND LinitisPlastica is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,9 as Pathology_Result_Type_Id
,NonSmallCellName  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(NonSmallCellName)) <>'' AND NonSmallCellName is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,10 as Pathology_Result_Type_Id
,PanCoast  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(PanCoast)) <>'' AND PanCoast is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,11 as Pathology_Result_Type_Id
,ALK  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(ALK)) <>'' AND ALK is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,12 as Pathology_Result_Type_Id
,EGFR  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(EGFR)) <>'' AND EGFR is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,13 as Pathology_Result_Type_Id
,ROS  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(ROS)) <>'' AND ROS is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,14 as Pathology_Result_Type_Id
,PIK3CA  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(PIK3CA)) <>'' AND PIK3CA is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,15 as Pathology_Result_Type_Id
,MET  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(MET)) <>'' AND MET is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,16 as Pathology_Result_Type_Id
,Her2NeuIHC  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(Her2NeuIHC)) <>'' AND Her2NeuIHC is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,17 as Pathology_Result_Type_Id
,FISH  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where ltrim(rtrim(FISH)) <>'' AND FISH is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,18 as Pathology_Result_Type_Id
,AxillaryNodesTested  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(AxillaryNodesTested)) <>'' AND AxillaryNodesTested is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,19 as Pathology_Result_Type_Id
,Mucinous  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where ltrim(rtrim(Mucinous)) <>'' AND Mucinous is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,20 as Pathology_Result_Type_Id
,Comedonecrosis  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where ltrim(rtrim(Comedonecrosis)) <>'' AND Comedonecrosis is not null  
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,21 as Pathology_Result_Type_Id
,FishEquivocal  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(FishEquivocal)) <>'' AND FishEquivocal is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,22 as Pathology_Result_Type_Id
,cast (AnyLocalEvent_DCIS_Inv_pct as varchar(50)) as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where  AnyLocalEvent_DCIS_Inv_pct is not null 
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,23 as Pathology_Result_Type_Id
,'10' as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where \"10yrRecurrenceRatePct\" is not null  
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,24 as Pathology_Result_Type_Id
,cast(InvasiveLocalEventPct as varchar(50))  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK)
where  InvasiveLocalEventPct is not null  
UNION
Select 
PatientPathologyResultFactID as CN_Patient_Pathology_Res_SID
,CoreRecordID as Core_Record_Type_Id
,PatientDimId as Nav_Patient_Id
,TumorTypeDimId as Tumor_Type_Id
,DiagnosisResultID as Diagnosis_Result_Id
,DiagnosisDimID as Nav_Diagnosis_Id
,NavigatorDimID as Navigator_Id
,Coid as Coid
,'H' as Company_Code
,25 as Pathology_Result_Type_Id
,ERPR  as Result_Value_Text
,concat('0x',CONVERT(varchar(50),HBSource,2)) as Hashbite_SSK
from NavAdhoc.dbo.PatientPathologyResults (NOLOCK) 
where ltrim(rtrim(ERPR)) <>'' AND ERPR is not null 
)ab "

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_PATHOLOGY_RESULT_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
from ${NCR_STG_SCHEMA}.CN_PATIENT_PATHOLOGY_RESULT_STG"


