#####################################################################################
#										    #
#	Script Name 	- HDW_TMS_Ref_Future_Role_Attribute_Wrk.SQL		    #
#	Job Name 	- J_HDW_TMS_Ref_Future_Role_Attribute_Wrk		    #
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

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

 
 delete from EDWHR_STAGING.Ref_Future_Role_Attribute_Wrk;
 
 insert into EDWHR_STAGING.Ref_Future_Role_Attribute_Wrk
 select distinct coalesce(A.role_id,'U'), 'M' as source_system_code, current_timestamp(0) as dw_last_update_date_time
 from 
 (select distinct TRIM(Future_Role1_Leadership_Level) as role_id 
from EDWHR_STAGING.Employee_Info 
union all
select distinct TRIM(Future_Role1_Org_Size_Scope) as role_id 
from EDWHR_STAGING.Employee_Info
union all
select distinct TRIM(Future_Role1_R_Timeframe) as role_id 
from EDWHR_STAGING.Employee_Info 
union all
select distinct TRIM(Future_Role1_R_Function_Area)  as role_id 
from EDWHR_STAGING.Employee_Info
union all
select distinct TRIM(Future_Role2_Leadership_Level)  as role_id 
from EDWHR_STAGING.Employee_Info
union all
select distinct TRIM(Future_Role2_Org_Size_Scope)  as role_id 
from EDWHR_STAGING.Employee_Info
union all
select distinct TRIM(Future_Role2_R_Timeframe)  as role_id 
from EDWHR_STAGING.Employee_Info
union all
select distinct TRIM(Future_Role2_R_Function_Area) as role_id
from EDWHR_STAGING.Employee_Info) A;


.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

.Logoff;

.exit

EOF


