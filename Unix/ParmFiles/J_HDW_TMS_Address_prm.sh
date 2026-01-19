##########################
## Variable Declaration ##
##########################

export AC_EXP_SQL_STATEMENT="select '$Job_Name'||','||
cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
(
SELECT '1' AS cnt FROM
(
SELECT 
STG.SK AS Addr_SID,
STG.Addr_Type_Cd AS Addr_Type_Code,
STG.Addr1 Addr_Line_1_Text,
STG.Addr2 AS Addr_Line_2_Text,
STG.Addr3 AS Addr_Line_3_Text,
STG.Addr4 AS Addr_Line_4_Text,
STG.City AS City_Name,
STG.State AS State_Code,
STG.Zip AS Zip_Code,
STG.County AS County_Name,
STG.Country_Code AS Country_Code,
STG.Location_Code AS Location_Code,
STG.Source_System_Code AS Source_System_Code,
CURRENT_TIMESTAMP(0) as DW_Last_Update_Date_Time
FROM
(
SELECT  DISTINCT
XWLK.SK,
'PWR' as Addr_Type_Cd,
TRIM(COALESCE(PWR.Work_History_Address,'')) AS Addr1,
NULL(CHAR) AS Addr2,
NULL(CHAR) AS Addr3,
NULL(CHAR) AS Addr4,
TRIM(COALESCE(PWR.Work_History_City,'')) AS City,
'' AS State,
TRIM(COALESCE(PWR.Work_History_Postal_Code,'')) AS Zip,
'' AS County,
TRIM(COALESCE(PWR.Work_History_Country,'')) AS Country_Code,
NULL(CHAR) AS Location_Code,
'M' as Source_System_Code
FROM EDWHR_STAGING.WORK_HISTORY_REPORT PWR
INNER JOIN EDWHR_STAGING.Ref_SK_Xwlk XWLK
ON ('PWR' ||'-'|| TRIM(COALESCE(PWR.Work_History_Address,''))||'-'|| TRIM(COALESCE(PWR.Work_History_City,''))||'-'||  TRIM(COALESCE(PWR.Work_History_Postal_Code,''))||'-'||  TRIM(COALESCE(PWR.Work_History_Country,''))) = XWLK.SK_Source_Txt  AND XWLK.SK_Type = 'Address'

) STG
LEFT OUTER JOIN EDWHR.ADDRESS TGT
ON STG.SK = TGT.ADDR_SID 
WHERE TGT.ADDR_SID IS NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
) P
) O"

export AC_ACT_SQL_STATEMENT="select '$Job_Name'||','||
cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Address WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM ${NCR_AC_VIEW}.ETL_JOB_RUN where Job_Name = '$Job_Name')"