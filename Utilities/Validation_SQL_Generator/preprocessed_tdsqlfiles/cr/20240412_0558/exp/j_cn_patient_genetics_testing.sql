SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT GeneticsTestingFactID,
 CoreRecordID,
 PatientDimID,
 TumorTypeDimId,
 DiagnosisResultId,
 DiagnosisDimID,
 Coid,
 'H' AS Company_Code,
 NavigatorDimId,
 GeneticsDate,
 GeneticsTestType,
 GeneticsSpecialist,
 Breast_Cancer_Type_ID,
 GeneticsComments,
 HBSource,
 'N' Source_System_Code,
 Current_Timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Patient_Genetics_Testing_STG PGT
 LEFT OUTER JOIN edwcr.Ref_Breast_Cancer_Type BRCA ON PGT.GeneticsBRCAType=BRCA. Breast_Cancer_Type_Desc
 WHERE HBSource NOT IN
 (SELECT Hashbite_SSK
 FROM edwcr.CN_Patient_Genetics_Testing) ) SRC