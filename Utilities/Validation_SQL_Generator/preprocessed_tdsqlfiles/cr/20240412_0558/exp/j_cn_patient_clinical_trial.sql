SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT CN_Patient_Clinical_Trial_SID ,
 Nav_Patient_Id ,
 Tumor_Type_Id ,
 Diagnosis_Result_Id ,
 Nav_Diagnosis_Id ,
 Navigator_Id ,
 Coid ,
 Company_Code ,
 Clinical_Trial_Name ,
 Clinical_Trial_Enrolled_Ind ,
 Clinical_Trial_Enrolled_Date ,
 Clinical_Trial_Offered_Ind ,
 Clinical_Trial_Offered_Date ,
 Hashbite_SSK ,
 Source_System_Code ,
 Current_timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Patient_Clinical_Trial_Stg
 WHERE Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_Patient_Clinical_Trial) ) A;