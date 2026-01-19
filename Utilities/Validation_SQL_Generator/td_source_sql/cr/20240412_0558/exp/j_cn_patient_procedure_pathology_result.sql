Select 'J_CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 
(
Select 
ROW_NUMBER() over (order by STG.CN_Patient_Procedure_SID,RRE.Nav_Result_Id,STG.Navigation_Procedure_Type_Code,STG.Pathology_Result_Date
,STG.Pathology_Result_Name,STG.Pathology_Grade_Available_Ind,STG.Pathology_Grade_Num,STG.Pathology_Tumor_Size_Av_Ind,STG.Tumor_Size_Num_Text,STG.Margin_Result_Detail_Text
,STG.Sentinel_Node_Result_Code,STG.Estrogen_Receptor_Sw,STG.Estrogen_Receptor_St_Cd,STG.Estrogen_Receptor_Pct_Text,STG.Progesterone_Receptor_Sw,STG.Progesterone_Receptor_St_Cd
,STG.Progesterone_Receptor_Pct_Text,STG.Oncotype_Diagnosis_Score_Num,STG.Oncotype_Diagnosis_Risk_Text,STG.Comment_Text,STG.Hashbite_SSK) + (SEL COALESCE(MAX(CN_Patient_Proc_Pathology_Result_SID), 0) AS ID1 FROM  EDWCR.CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT) AS  CN_Patient_Proc_Pathology_Result_SID
,STG.CN_Patient_Procedure_SID as CN_Patient_Procedure_SID
,RRE.Nav_Result_Id as Margin_Result_Id
,RRE.Nav_Result_Id as Nav_Result_Id
,RRE.Nav_Result_Id as Oncotype_Diagnosis_Result_Id
,STG.Navigation_Procedure_Type_Code
,STG.Pathology_Result_Date
,STG.Pathology_Result_Name
,STG.Pathology_Grade_Available_Ind
,STG.Pathology_Grade_Num
,STG.Pathology_Tumor_Size_Av_Ind
,STG.Tumor_Size_Num_Text
,STG.Margin_Result_Detail_Text
,STG.Sentinel_Node_Result_Code
,STG.Estrogen_Receptor_Sw
,STG.Estrogen_Receptor_St_Cd
,STG.Estrogen_Receptor_Pct_Text
,STG.Progesterone_Receptor_Sw
,STG.Progesterone_Receptor_St_Cd
,STG.Progesterone_Receptor_Pct_Text
,STG.Oncotype_Diagnosis_Score_Num
,STG.Oncotype_Diagnosis_Risk_Text
,STG.Comment_Text
,STG.Hashbite_SSK
,STG.Source_System_Code,
Current_timestamp(0) as DW_Last_Update_Date_Time
from edwcr_staging.CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT_STG STG
Left Outer Join edwcr_base_views.Ref_Result RRE
On
STG.Nav_Result_Id=RRE.Nav_Result_Desc
Left Outer Join edwcr_base_views.Ref_Result RRE1
On
STG.Margin_Result_Id=RRE1.Nav_Result_Desc
Left Outer Join edwcr_base_views.Ref_Result RRE2
On
STG.Oncotype_Diagnosis_Result_Id=RRE2.Nav_Result_Desc
where (Hashbite_SSK,Navigation_Procedure_Type_Code)  not in (Select Hashbite_SSK,Navigation_Procedure_Type_Code from edwcr.CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT )
and STG.DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM edwcr_dmx_ac_base_views.ETL_JOB_RUN where Job_Name = 'J_CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT')
) SRC