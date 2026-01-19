SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT Nav_Survivorship_Plan_Task_SID,
 REF2.Status_ID,
 REF1.Contact_Method_ID,
 Nav_Patient_Id,
 Navigator_Id,
 Coid,
 Company_Code,
 Task_Desc_Text,
 Task_Resolution_Date,
 Task_Closed_Date,
 Contact_Result_Text,
 Contact_Date,
 Comment_Text,
 Hashbite_SSK,
 STG.Source_System_Code,
 STG.DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Patient_Survivorship_Plan_Task_STG STG
 LEFT OUTER JOIN edwcr.REF_CONTACT_METHOD REF1 ON STG.TaskMeasnOfContact = REF1.Contact_Method_Desc
 LEFT OUTER JOIN edwcr.REF_STATUS REF2 ON STG.TASKSTATE = REF2.Status_Desc
 WHERE STG.Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_Patient_Survivorship_Plan_Task) )A;