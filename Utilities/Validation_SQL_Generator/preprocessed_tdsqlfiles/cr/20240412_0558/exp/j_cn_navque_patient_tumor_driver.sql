SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM EDWCR_BASE_VIEWS.NavQue_History A
LEFT OUTER JOIN (Sel CPIO.*
 FROM
 (SELECT DISTINCT A.Cancer_Patient_Driver_SK,
 A.Message_Control_Id_Text,
 A.User_Action_Date_Time
 FROM EDWCR_BASE_VIEWS.Cancer_Patient_Id_Output_Driver A) CPIO qualify row_number() over(PARTITION BY CPIO.Message_Control_Id_Text
 ORDER BY CPIO.User_Action_Date_Time DESC)=1)SK ON A.Message_Control_Id_Text=SK.Message_Control_Id_Text
LEFT OUTER JOIN (Sel cp_icd_oncology_code,
 CN_Navque_Tumor_Type_Id,
 cancer_tumor_driver_sk
 FROM edwcr_base_views.cancer_tumor_driver qualify row_number() over(PARTITION BY CN_Navque_Tumor_Type_Id
 ORDER BY cancer_tumor_driver_sk DESC)=1) CTD ON A.Tumor_type_Id=CTD.CN_Navque_Tumor_Type_Id ;