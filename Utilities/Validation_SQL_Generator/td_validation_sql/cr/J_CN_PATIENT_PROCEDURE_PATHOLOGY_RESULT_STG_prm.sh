export JOBNAME='J_CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING 
From
(
SELECT 
PatientSurgeryFactId as CN_Patient_Procedure_SID,
'S' as Navigation_Procedure_Type_Code,
SurgeryPathResultType as Nav_Result_Id,
SurgeryPathResultDate as Pathology_Result_Date,
SurgeryPathResultName as Pathology_Result_Name,
CASE 
WHEN UPPER(ltrim(rtrim(SurgeryPathGradeAvailable))) = 'YES' then 'Y' 
WHEN UPPER(ltrim(rtrim(SurgeryPathGradeAvailable))) = 'NO'  then 'N' else null
END as Pathology_Grade_Available_Ind,
CAST( CASE WHEN SurgeryPathGrade = 'I' THEN 1
WHEN SurgeryPathGrade = 'II' THEN 2
WHEN SurgeryPathGrade = 'III' THEN 3 else NULL
END as INT) as Pathology_Grade_Num,
CASE 
WHEN UPPER(ltrim(rtrim(SurgeryPathTumorSizeAvailable))) = 'YES' then 'Y'
WHEN UPPER(ltrim(rtrim(SurgeryPathTumorSizeAvailable))) = 'NO'  then 'N' else null
END as Pathology_Tumor_Size_Av_Ind, 
CAST(SurgeryPathTumorSize AS VARCHAR(30)) AS Tumor_Size_Num_Text,
SurgeryPathMarginResult AS Margin_Result_Id,
SurgeryPathMarginDetail AS Margin_Result_Detail_Text,
SurgeryPathSentinelNodeResult AS Sentinel_Node_Result_Code,
CAST (CASE When UPPER(ltrim(rtrim(SurgeryPathReceptorsER))) ='POS' then 1 
When UPPER(ltrim(rtrim(SurgeryPathReceptorsER))) ='NEG' then 0 else null END AS INTEGER) as  Receptor_ER_Sw, 
SurgeryPathReceptorsERStrength as Estrogen_Receptor_St_Cd,
CASt(SurgeryPathReceptorsERpct as VARCHAR(20)) as Receptor_ER_Pct_Text,
CAST (CASE When UPPER(ltrim(rtrim(SurgeryPathReceptorsPR))) ='POS' then 1 
When UPPER(ltrim(rtrim(SurgeryPathReceptorsPR))) ='NEG' then 0 else null END  AS INTEGER) AS Receptor_PR_Sw, 
SurgeryPathReceptorsPRStrength as Progesterone_Receptor_St_Cd, 
CAST(SurgeryPathReceptorsPRpct AS VARCHAR(20))  as Receptor_PR_Pct_Text, 
SurgeryPathOncoTypeDxResult AS Oncotype_Diagnosis_Result_Id,
SurgeryPathOncoTypeDxScore as Oncotype_Diagnosis_Score_Num,
SurgeryPathOncoTypeDxRisk as Oncotype_Diagnosis_Risk_Text,
ltrim(rtrim(cast(SurgeryPathComments as varchar(4000)))) AS Comment_Text,
concat('0x',CONVERT(varchar(50),HBSource,2))  AS Hashbite_SSK
FROM NavAdhoc.dbo.PatientSurgery (NOLOCK)

UNION 

SELECT 
PatientBiopsyFactID as CN_Patient_Procedure_SID,
'B' as Navigation_Procedure_Type_Code,
BxPathResultType as Nav_Result_Id,
BxPathResultDate as Pathology_Result_Date,
BxPathResultName as Pathology_Result_Name,
NULL as Pathology_Grade_Available_Ind,
CAST(CASE WHEN BxPathGrade = 'I' THEN 1
WHEN BxPathGrade = 'II' THEN 2
WHEN BxPathGrade = 'III' THEN 3 else NULL
END as INT) AS Pathology_Grade_Num,
NULL as Pathology_Tumor_Size_Av_Ind,
CAST (BxPathTumorSize AS VARCHAR(30)) AS Tumor_Size_Num_Text, 
BxPathMarginResult AS Margin_Result_Id,
BxPathMarginDetail AS Margin_Result_Detail_Text,
BxPathSentinelNodeResult AS Sentinel_Node_Result_Code,
CAST(CASE When UPPER(ltrim(rtrim(BxPathReceptorsER))) ='POS' then 1 
When UPPER(ltrim(rtrim(BxPathReceptorsER))) ='NEG' then 0 else null end  AS INTEGER) AS Receptor_ER_Sw, 
BxPathReceptorsERStrength AS Estrogen_Receptor_St_Cd,
CAST(BxPathReceptorsERpct AS VARCHAR(20)) AS Receptor_ER_Pct_Text,
CAST(CASE When UPPER(ltrim(rtrim(BxPathReceptorsPR))) ='POS' then 1 
When UPPER(ltrim(rtrim(BxPathReceptorsPR))) ='NEG' then 0 else null end  AS INTEGER) AS Receptor_PR_Sw,
BxPathReceptorsPRstrength AS Progesterone_Receptor_St_Cd,
CAST(BxPathReceptorsPRpct AS VARCHAR(20)) AS Receptor_PR_Pct_Text,
BxPathOncoTypeDxResult as Oncotype_Diagnosis_Result_Id,
BxPathOncoTypeDxScore AS Oncotype_Diagnosis_Score_Num,
BxPathOncoTypeDxRisk as Oncotype_Diagnosis_Risk_Text,
ltrim(rtrim(cast(BxPathComments AS varchar(4000)))) as Comment_Text,
concat('0x',CONVERT(varchar(50),HBSource,2))  AS Hashbite_SSK
FROM NavAdhoc.dbo.PatientBiopsy (NOLOCK)

) A"

export AC_ACT_SQL_STATEMENT="Select 'J_CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT_STG"
