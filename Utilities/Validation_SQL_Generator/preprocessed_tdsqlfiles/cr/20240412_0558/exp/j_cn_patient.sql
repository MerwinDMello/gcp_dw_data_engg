SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT STG.Nav_Patient_Id,
 STG.Navigator_Id,
 CASE
 WHEN TRIM(STG.Coid) IS NULL THEN '-1'
 ELSE STG.COID
 END AS COID,
 'H' AS Company_Code,
 STG.Patient_market_URN,
 STG.Medical_Record_Num,
 STG.EMPI_Text,
 PD1.Physician_ID AS Gynecologist_Physician_Id,
 PD2.Physician_ID AS Primary_Care_Physician_Id,
 CF.Facility_Mnemonic_CS AS Facility_Mnemonic_CS,
 CF.Network_Mnemonic_CS AS Network_Mnemonic_CS,
 STG.Nav_Create_Date,
 'N' AS Source_System_Code,
 Current_Timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Patient_STG STG
 LEFT OUTER JOIN edwcr.CN_Physician_Detail PD1 ON COALESCE(TRIM(STG.Gynecologist), 'X') = COALESCE(TRIM(PD1.Physician_Name), 'X')
 AND COALESCE(TRIM(STG.GynecologistPhone), 'X') = COALESCE(TRIM(PD1.Physician_Phone_Num), 'X')
 LEFT OUTER JOIN edwcr.CN_Physician_Detail PD2 ON COALESCE(TRIM(STG.PrimaryCarePhysician), 'XX') = COALESCE(TRIM(PD2.Physician_Name), 'XX')
 AND COALESCE(TRIM(STG.PCPPhone), 'XX') = COALESCE(TRIM(PD2.Physician_Phone_Num), 'XX')
 LEFT OUTER JOIN edw_pub_views.clinical_Facility CF ON STG.COID = CF.COID
 AND CF.Facility_Active_Ind='Y' --where STG.COID is NOT NULL

 WHERE STG.Nav_Patient_Id NOT IN
 (SELECT Nav_Patient_Id
 FROM edwcr.CN_Patient) qualify row_number() over(PARTITION BY STG.Nav_Patient_Id
 ORDER BY Primary_Care_Physician_Id DESC)=1 ) SRC