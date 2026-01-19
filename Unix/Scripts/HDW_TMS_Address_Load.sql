#####################################################################################
#												                                   #
#	Script Name 	- HDW_TMS_Address_Load.SQL						                   #
#	Job Name 	- J_HDW_TMS_Address							                           #
#	Target Table 	- EDWHR.Address							           #
#	Developer	- Julia Kim
#	Version	- 1.0 - Initial Release						                           #
#	Description	- The script loads the core table with only new records		       #
#												                                   #
#####################################################################################

bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Address;' FOR SESSION;

/*	Generate the surrogate keys for Address	*/

CALL EDWHR_Procs.SK_GEN('EDWHR_STAGING','Work_History_Report','''PWR'' ||''-''|| TRIM(COALESCE(Work_History_Address,''''))||''-''|| TRIM(COALESCE( Work_History_City,''''))||''-''|| TRIM(COALESCE( Work_History_Postal_Code,''''))||''-''|| TRIM(COALESCE( Work_History_Country,'''')) ', 'Address');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/* Collecting Stats on look-up table */

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR_STAGING','REF_SK_XWLK');

/*	Load only the new records to the Core Table	*/

INSERT INTO EDWHR.Address
(
Addr_SID,
Addr_Type_Code,
Addr_Line_1_Text,
Addr_Line_2_Text,
Addr_Line_3_Text,
Addr_Line_4_Text,
City_Name,
State_Code,
Zip_Code,
County_Name,
Country_Code,
Location_Code,
Source_System_Code,
DW_Last_Update_Date_Time
)
SELECT 
STG.SK AS Addr_SID,
TRIM(STG.Addr_Type_Cd) AS Addr_Type_Code,
STG.Addr1 AS Addr_Line_1_Text,
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
LEFT OUTER JOIN EDWHR.Address TGT
ON STG.SK = TGT.Addr_SID 
WHERE TGT.Addr_SID IS NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14;


.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/*	Collect Statistics on the Core Table	*/

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','ADDRESS');

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

.Logoff;

.exit

EOF


