SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT STG.NavQue_History_Id ,
 STG.NavQue_Action_Id ,
 STG.NavQue_Reason_Id ,
 STG.Tumor_Type_Id ,
 STG.Navigator_Id ,
 STG.Coid ,
 STG.Company_Code ,
 STG.Message_Control_Id_Text ,
 STG.Message_Date ,
 STG.NavQue_Insert_Date ,
 STG.NavQue_Action_Date ,
 STG.Medical_Record_Num ,
 STG.Patient_Market_URN ,
 STG.Network_Mnemonic_CS ,
 STG.Transition_Of_Care_Score_Num ,
 STG.Navigated_Patient_Ind ,
 STG.Message_Source_Flag ,
 STG.Hashbite_SSK ,
 STG.Source_System_Code ,
 current_timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.NavQue_History_STG STG
 WHERE trim(STG.Hashbite_SSK) NOT IN
 (SELECT trim(Hashbite_SSK)
 FROM edwcr.NavQue_History) ) A