#####################################################################################
#                                               					#
#   SCRIPT NAME     - HDW_TMS_Ref_Location_Willingness.SQL                  	
#   Job NAME    	- J_HDW_TMS_Ref_Location_Willingness                        	#
#   TARGET TABLE    - $NCR_TGT_SCHEMA.Ref_Location_Willingness                     	#
#   Developer   	- Julia Kim                                 			#
#   Version 		- 1.0 - Initial RELEASE                     			#
#   Description 	- The SCRIPT loads the TARGET TABLE WITH NEW records		#
#####################################################################################


bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Ref_Location_Willingness;' FOR SESSION;

/* Load Work Table with working Data */

DELETE FROM EDWHR_STAGING.Ref_Location_Willingness_WRK;


INSERT INTO $NCR_STG_SCHEMA.Ref_Location_Willingness_WRK
(
Location_Willingness_Id,
Location_Willingness_Desc,
Source_System_Code,
DW_Last_Update_Date_Time
)
SELECT 
(SELECT (COALESCE(MAX(Location_Willingness_Id),0)) FROM EDWHR_BASE_VIEWS.Ref_Location_Willingness) + 
ROW_NUMBER() OVER (ORDER BY Y.Location_Willingness_Desc) AS Location_Willingness_Id
, Y.Location_Willingness_Desc
, 'M' AS SOURCE_SYSTEM_CODE
, CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM (
SEL STG.Location_Willingness_Desc
FROM 
(   
SELECT 
DISTINCT(TRIM(Willing_To_Relocate)) AS Location_Willingness_Desc
FROM $NCR_STG_SCHEMA.EMPLOYEE_INFO 
WHERE TRIM(Willing_To_Relocate) IS NOT NULL AND TRIM(Willing_To_Relocate) NOT = ''

UNION 

SELECT 
DISTINCT(TRIM(Willing_To_Travel)) AS Location_Willingness_Desc
FROM $NCR_STG_SCHEMA.EMPLOYEE_INFO 
WHERE TRIM(Willing_To_Travel) IS NOT NULL AND TRIM(Willing_To_Travel) NOT = ''

) STG

LEFT OUTER JOIN EDWHR_Staging.Ref_Location_Willingness_WRK TGT
ON SUBSTR(STG.Location_Willingness_Desc,1,100) = TGT.Location_Willingness_Desc

WHERE TGT.Location_Willingness_Desc IS NULL
) Y
;




.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
/*  Collect Statistics on the WRK Table    */

CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Ref_Location_Willingness_WRK');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


Insert into EDWHR.Ref_Location_Willingness ( 
Location_Willingness_Id,
Location_Willingness_Desc,
Source_System_Code,
DW_Last_Update_Date_Time
 )

select Row_Number() Over ( ORDER BY WRK.Location_Willingness_Id) + (SELECT Coalesce(Max(Location_Willingness_Id),0) FROM EDWHR.Ref_Location_Willingness) AS Location_Willingness_Id,
Trim(WRK.Location_Willingness_Desc),
WRK.SOURCE_SYSTEM_CODE ,
Current_Timestamp(0) as DW_Last_Update_Date_Time
FROM EDWHR_STAGING.Ref_Location_Willingness_WRK WRK
where  Trim(WRK.Location_Willingness_Desc) not in (sel Location_Willingness_Desc from EDWHR.Ref_Location_Willingness); 



/*  Collect Statistics on the Target Table    */

CALL dbadmin_procs.collect_stats_table ('$NCR_TGT_SCHEMA','Ref_Location_Willingness');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;

.EXIT

EOF
