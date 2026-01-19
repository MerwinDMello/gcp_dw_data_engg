SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT STG.PatientHemeFactID AS PatientHemeFactID,
 STG.PatientDimID AS PatientDimID,
 STG.TumorTypeDimID AS TumorTypeDimID,
 STG.DiagnosisResultID AS DiagnosisResultID,
 STG.DiagnosisDimID AS DiagnosisDimID,
 STG.COID AS COID,
 'H' Company_code,
 STG.NavigatorDimID AS NavigatorDimID,
 STG.Transportation AS Transportation,
 STG.DrugUseHistory AS DrugUseHistory,
 DTL.Physician_Id AS Physician_Id,
 STG.HBSource AS HBSource,
 'N' AS Source_System_Code,
 Current_Timestamp(0) AS DW_Last_Update_Date_Time
 FROM EDWCR_STaging.CN_Patient_Heme_STG STG
 LEFT JOIN EDWCR.CN_Physician_Detail DTL ON DTL.Physician_Name = STG.Hematologist
 WHERE STG.HBSource NOT IN
 (SELECT TGT.Hashbite_SSK
 FROM EDWCR.CN_Patient_Heme TGT)
 GROUP BY 1,
 2,
 3,
 4,
 5,
 6,
 7,
 8,
 9,
 10,
 11,
 12,
 13,
 14 qualify row_number() OVER (PARTITION BY PatientHemeFactID
 ORDER BY DTL.Physician_Id DESC)=1) a