SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT row_number() OVER (
 ORDER BY SK.Cancer_Patient_Driver_SK ASC) AS Cancer_Patient_Id_Input_Driver_SK,
 SK.Cancer_Patient_Driver_SK,
 A.Message_Control_Id_Text,
 A.Patient_Type_Status_SK,
 A.Coid,
 A.Company_Code,
 A.Patient_DW_Id,
 A.Pat_Acct_Num,
 A.Medical_Record_Num,
 A.Patient_Market_URN,
 A.Message_Type_Code,
 A.Message_Flag_Code,
 A.Message_Event_Type_Code,
 A.Message_Origin_Requested_Date_Time,
 A.Message_Signed_Observation_Date_Time,
 A.Message_Created_Date_Time,
 A.First_Insert_Date_Time,
 'H' AS Source_System_Code,
 current_timestamp(0) AS DW_Last_Update_Date_Time
 FROM EDWCR_BASE_VIEWS.Cancer_Patient_Id_Input A
 LEFT OUTER JOIN
 (SELECT A1.Cancer_Patient_Driver_SK,
 A1.Message_Control_Id_Text
 FROM EDWCR_BASE_VIEWS.Cancer_Patient_Id_Output_Driver A1 qualify row_number() over(PARTITION BY A1.Message_Control_Id_Text
 ORDER BY A1.User_Action_Date_Time DESC)=1) SK ON A.Message_Control_Id_Text=SK.Message_Control_Id_Text)STG