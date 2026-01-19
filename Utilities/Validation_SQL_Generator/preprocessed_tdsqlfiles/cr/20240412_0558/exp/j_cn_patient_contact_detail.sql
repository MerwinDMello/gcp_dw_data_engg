SELECT CONCAT(count(*), ',	') AS SOURCE_STRING
FROM
 (SELECT CN_Patient_Contact_SID,
 Contact_Detail_Measure_Type_Id,
 Contact_Detail_Measure_Val_Txt,
 Hashbite_SSK,
 Source_System_Code,
 DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_PATIENT_CONTACT_DETAIL_STG STG
 WHERE STG.Hashbite_SSK NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_PATIENT_CONTACT_DETAIL) )A;