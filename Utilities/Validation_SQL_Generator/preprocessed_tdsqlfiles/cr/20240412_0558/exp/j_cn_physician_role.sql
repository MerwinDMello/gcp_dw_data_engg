SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT Physician_Id ,
 Physician_Role_Code
 FROM edwcr_staging.CN_Physician_Role_Stg
 UNION SELECT Physician_Id,
 'Gyn'
 FROM edwcr.CN_Physician_Detail
 INNER JOIN edwcr.CN_patient ON Physician_Id=Gynecologist_Physician_Id
 UNION SELECT Physician_Id,
 'PCP'
 FROM edwcr.CN_Physician_Detail
 INNER JOIN edwcr.CN_patient ON Physician_Id=Primary_Care_Physician_Id
 UNION SELECT Physician_Id,
 'ETP'
 FROM edwcr.CN_Physician_Detail
 INNER JOIN edwcr.CN_Patient_Tumor ON Physician_Id=Treatment_End_Physician_Id)ab
WHERE (Physician_Id,
 Physician_Role_Code) NOT IN
 (SELECT Physician_Id,
 Physician_Role_Code
 FROM edwcr.CN_PHYSICIAN_ROLE)