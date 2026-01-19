SELECT CONCAT(count(*)) AS SOURCE_STRING
FROM
 (SELECT ROW_NUMBER() OVER(
 ORDER BY WRK.Physician_Name, WRK.Physician_Phone_Num)+(SEL COALESCE(MAX(Physician_ID), 900000) AS Physician_ID
 FROM edwcr.CN_Physician_Detail) AS Physician_ID,
 WRK.Physician_Name,
 WRK.Physician_Phone_Num,
 'N' AS Source_System_Code,
 Current_Timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Physician_Detail_STG_WRK WRK
 LEFT OUTER JOIN
 (SELECT Physician_Id,
 Physician_Name,
 Physician_Phone_Num
 FROM edwcr.CN_Physician_Detail
 WHERE Physician_Id>900000 ) TGT ON COALESCE(TRIM(WRK.Physician_Name), 'XX')=COALESCE(TRIM(TGT.Physician_Name), 'XX')
 AND COALESCE(TRIM(WRK.Physician_Phone_Num), 'X')=COALESCE(TRIM(TGT.Physician_Phone_Num), 'X')
 WHERE TGT.Physician_ID IS NULL
 AND WRK.Physician_ID IS NULL
 UNION ALL SELECT WRK.Physician_ID,
 WRK.Physician_Name,
 WRK.Physician_Phone_Num,
 'N' AS Source_System_Code,
 Current_Timestamp(0) AS DW_Last_Update_Date_Time
 FROM edwcr_staging.CN_Physician_Detail_STG_WRK WRK
 LEFT OUTER JOIN
 (SELECT Physician_Id,
 Physician_Name,
 Physician_Phone_Num
 FROM edwcr.CN_Physician_Detail
 WHERE Physician_Id<=900000 ) TGT ON COALESCE(TRIM(WRK.Physician_Name), 'XX')=COALESCE(TRIM(TGT.Physician_Name), 'XX')
 AND COALESCE(TRIM(WRK.Physician_Phone_Num), 'X')=COALESCE(TRIM(TGT.Physician_Phone_Num), 'X')
 WHERE TGT.Physician_ID IS NULL
 AND WRK.Physician_ID IS NOT NULL ) SRC