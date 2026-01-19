SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT CN_Patient_Mltp_Disc_Meet_SID,
 Nav_Patient_Id,
 Tumor_Type_Id,
 Navigator_Id,
 Coid,
 Company_Code,
 Meeting_Date,
 Patient_Discussed_Ind,
 Treatment_Change_Ind,
 Meeting_Notes_Text,
 Hashbite_SSK,
 Source_System_Code,
 DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Patient_Mltp_Disciplinary_Meeting_STG STG
 WHERE STG.Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_Patient_Mltp_Disciplinary_Meeting) )A;