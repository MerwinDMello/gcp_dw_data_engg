SELECT 'J_CN_NAVQUE_PATIENT_TUMOR_DRIVER'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_BASE_VIEWS.NavQue_History A
Left Outer Join
(Sel CPIO.* from
(SELECT  
distinct
A.Cancer_Patient_Driver_SK,
A.Message_Control_Id_Text,
A.User_Action_Date_Time
FROM EDWCR_BASE_VIEWS.Cancer_Patient_Id_Output_Driver A
) CPIO
qualify row_number() over(partition by CPIO.Message_Control_Id_Text order by CPIO.User_Action_Date_Time desc)=1 )SK
on A.Message_Control_Id_Text=SK.Message_Control_Id_Text
Left Outer Join (
Sel cp_icd_oncology_code,CN_Navque_Tumor_Type_Id,cancer_tumor_driver_sk from 
edwcr_base_views.cancer_tumor_driver
qualify row_number() over(partition by CN_Navque_Tumor_Type_Id order by cancer_tumor_driver_sk desc)=1
) CTD
on  A.Tumor_type_Id=CTD.CN_Navque_Tumor_Type_Id
;