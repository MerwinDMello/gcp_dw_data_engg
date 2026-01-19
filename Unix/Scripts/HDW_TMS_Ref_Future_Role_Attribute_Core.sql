#####################################################################################
#										    #
#	Script Name 	- HDW_TMS_Ref_Future_Role_Attribute_Core.SQL		    #
#	Job Name 	- J_HDW_TMS_Ref_Future_Role_Attribute			    #
#	Target Table 	- EDWHR.Ref_Future_Role_Attribute			    #
#	Developer	- Heather  						    #
#	Version	- 1.0 - Initial Release						    #
#	Description	- The script loads the core table with only new records	    #
#										    #
#####################################################################################

bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Ref_Future_Role_Attribute;' FOR SESSION;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*  Load Work Table with working Data */

Delete from EDWHR_STAGING.Ref_Future_Role_Attribute_Wrk where Future_Role_Attribute_Desc IN ('--------', '');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

Insert into EDWHR.Ref_Future_Role_Attribute ( Future_Role_Attribute_ID, Future_Role_Attribute_Desc, SOURCE_SYSTEM_CODE, DW_LAST_UPDATE_DATE_TIME )

SELECT Cast(Cast( 
(SELECT Coalesce(Max( Future_Role_Attribute_ID ),0) FROM 
EDWHR.Ref_Future_Role_Attribute 
)
 + Row_Number() Over ( ORDER BY WRK.Future_Role_Attribute_Desc ) AS INT) AS CHAR(10)) AS Future_Role_Attribute_ID,  Future_Role_Attribute_Desc ,WRK.SOURCE_SYSTEM_CODE ,
WRK.DW_LAST_UPDATE_DATE_TIME FROM EDWHR_STAGING.Ref_Future_Role_Attribute_Wrk WRK
where  WRK.Future_Role_Attribute_Desc
not in (sel Future_Role_Attribute_Desc from EDWHR.Ref_Future_Role_Attribute);


.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

.Logoff;

.exit

EOF


