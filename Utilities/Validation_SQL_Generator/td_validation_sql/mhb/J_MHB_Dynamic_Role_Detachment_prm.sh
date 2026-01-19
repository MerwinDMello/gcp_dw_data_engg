 #  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Dynamic_Role_Detachment'

export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Dynamic_Role_Detachment'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_Dynamic_Role_Detachment where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT 'J_MHB_Dynamic_Role_Detachment'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(

SELECT 
RDC.RDC_SID AS RDC_SID
,MU.User_Login_Name AS User_Login_Name
,cast(Detachment.DynamicRole_DateTime as timestamp(6)) AS Detach_Date_Time
,COALESCE(MU2.User_Login_Name, 'Unknown') AS Creator_Login_Name
,COALESCE(MUR2.MHB_User_Role_Sid, -1) AS Creator_MHB_User_Role_SID
,RMU2.MHB_Unit_Id AS Created_MHB_Unit_Id
,coalesce(CFL2.Location_Mnemonic_CS,'Unknown') AS Created_Location_Mnemonic_CS
,Coalesce(CFL2.Company_Code,'H') AS Creator_Company_Code
,coalesce(CFL2.Coid, '99999') AS Creator_Coid
,coalesce(RMUR.MHB_User_Role_Sid, -1) AS MHB_User_Role_SID
,RMU1.MHB_Unit_Id AS MHB_User_Signin_Unit_Id
,coalesce(CFL1.Location_Mnemonic_CS,'Unknown') AS Signin_Location_Mnemonic_CS
,Coalesce(CFL1.Company_Code,'H') AS Company_Code
,coalesce(CFL1.Coid, '99999') AS Coid
,Dynamic_Role.Dynamic_Role_SID AS Dynamic_Role_SID
,DETACHMENT.DynamicRole_Phone AS Dynamic_Role_Phone_Num
,DETACHMENT.Trail_Id AS MHB_Audit_Trail_Num
,'B' AS SOURCE_SYSTEM_CODE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME


FROM EDWCI_STAGING.VWDYNAMICROLEDETACHMENT DETACHMENT
INNER JOIN EDWCI_BASE_VIEWS.REF_MHB_REGIONAL_DATA_CENTER RDC
ON SUBSTR(TRIM(DETACHMENT.DATABASENAME), POSITION('_' IN TRIM(DETACHMENT.DATABASENAME))+1) = RDC.RDC_DESC
INNER JOIN EDWCI_BASE_VIEWS.MHB_USER MU
ON DETACHMENT.USER_NAME = MU.USER_LOGIN_NAME 
AND RDC.RDC_SID=MU.RDC_SID
INNER JOIN EDWCI_BASE_VIEWS.REF_MHB_UNIT RMU2 
ON RMU2.RDC_SID = RDC.RDC_SID
AND DETACHMENT.CREATED_USER_UNIT_ID = RMU2.MHB_UNIT_ID
INNER JOIN  edwci_base_views.Ref_MHB_Unit RMU1
on RMU1.RDC_Sid = RDC.RDC_SID
AND DETACHMENT.User_Recent_Unit_Id = RMU1.MHB_Unit_Id
INNER JOIN   EDWCI_BASE_VIEWS.Ref_MHB_Dynamic_Role Dynamic_Role
ON Dynamic_Role.RDC_Sid = RDC.RDC_Sid
and Dynamic_Role.MHB_Dynamic_Role_Parent_Name = DETACHMENT.DynamicRole_Name 
and Dynamic_Role.MHB_Dynamic_Role_Child_Name = DETACHMENT.DynamicRole_Label
LEFT JOIN EDWCI_BASE_VIEWS.REF_MHB_USER_ROLE RMUR
ON TRIM(DETACHMENT.USER_ACTUAL_ROLE)  = TRIM(RMUR.MHB_USER_ROLE_DESC)
AND RMUR.MHB_USER_ROLE_SID = MU.MHB_USER_ROLE_SID
LEFT JOIN EDWCI_BASE_VIEWS.MHB_USER MU2
ON DETACHMENT.CREATED_USER_NAME = MU2.USER_LOGIN_NAME 
AND RDC.RDC_SID=MU2.RDC_SID
LEFT JOIN EDWCI_BASE_VIEWS.REF_MHB_USER_ROLE MUR2 
ON TRIM(DETACHMENT.CREATED_USER_ROLE)  = TRIM(MUR2.MHB_USER_ROLE_DESC)
AND MUR2.MHB_USER_ROLE_SID = MU2.MHB_USER_ROLE_SID
LEFT JOIN Edw_Pub_Views.Clinical_Facility_Location CFL2
on  trim(CFL2.Location_Mnemonic_CS) = trim(SUBSTR(Created_User_UnitCode,POSITION('_' IN Created_User_UnitCode)+1))
AND trim(CFL2.Facility_Mnemonic_CS) = trim(DETACHMENT.Created_User_FacilityCode)
LEFT JOIN Edw_Pub_Views.Clinical_Facility_Location CFL1
on  trim(CFL1.Location_Mnemonic_CS) = trim(SUBSTR(User_Recent_UnitCode,POSITION('_' IN User_Recent_UnitCode)+1))
AND trim(CFL1.Facility_Mnemonic_CS) = trim(DETACHMENT.User_Recent_FacilityCode)
LEFT JOIN EDWCI_BASE_VIEWS.MHB_Dynamic_Role_Detachment  Dynamic_Role_Detachment
ON Dynamic_Role_Detachment.RDC_SID = RDC.RDC_SID
AND Dynamic_Role_Detachment.User_Login_Name = MU.User_Login_Name
AND Dynamic_Role_Detachment.Detach_Date_Time = Detachment.DynamicRole_DateTime 

WHERE  CAST(DETACHMENT.DYNAMICROLE_DATETIME AS TIMESTAMP(6)) BETWEEN MU.EFF_FROM_DATE  AND MU.EFF_TO_DATE
AND CAST(DYNAMICROLE_DATETIME AS TIMESTAMP(6)) BETWEEN MU2.EFF_FROM_DATE  AND MU2.EFF_TO_DATE
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  



