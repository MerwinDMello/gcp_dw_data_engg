SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT PatientHistoryFactID AS CN_Patient_Family_History_SID,
 Family_History_Query_Id,
 PatientDimID AS Nav_Patient_Id,
 Coid,
 'H' AS Company_Code,
 Family_History_Value_Text,
 HBSource AS Hashbite_SSK,
 'N' AS Source_System_Code,
 Current_Timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Patient_Family_History_STG
 WHERE HBSource NOT IN
 (SELECT Hashbite_SSK
 FROM Edwcr_Base_Views.CN_PATIENT_FAMILY_HISTORY) ) SRC