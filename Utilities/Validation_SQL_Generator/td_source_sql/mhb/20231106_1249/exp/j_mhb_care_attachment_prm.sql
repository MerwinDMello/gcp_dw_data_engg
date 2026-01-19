SELECT 'J_MHB_Care_Attachment'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
SELECT 
RDC.RDC_SID AS RDC_SID
,MU.User_Login_Name AS User_Login_Name
,ck.Patient_DW_Id as Patient_DW_Id
,cast(Attachment.Attach_DateTime as timestamp(6)) AS Attach_Date_Time
,COALESCE(MU2.User_Login_Name, 'Unknown') AS Creator_Login_Name
,COALESCE(MUR2.MHB_User_Role_Sid, -1) AS Creator_MHB_User_Role_SID
,RMU2.MHB_Unit_Id AS Created_MHB_Unit_Id
,coalesce(CFL2.Location_Mnemonic_CS,'Unknown') AS Created_Location_Mnemonic_CS
,Coalesce(CFL2.Company_Code,'H') AS Creator_Company_Code
,coalesce(CFL2.Coid, '99999') AS Creator_Coid
,Coalesce(CF2.Company_Code,'H') AS Patient_Company_Code
,coalesce(CF2.Coid, '99999') AS Patient_Coid
,CK.Pat_Acct_Num AS Pat_Acct_Num
,coalesce(RMUR.MHB_User_Role_Sid, -1) AS MHB_User_Role_SID
,RMU1.MHB_Unit_Id AS MHB_User_Signin_Unit_Id
,coalesce(CFL1.Location_Mnemonic_CS,'Unknown') AS Signin_Location_Mnemonic_CS
,Coalesce(CFL1.Company_Code,'H') AS Company_Code
,coalesce(CFL1.Coid, '99999') AS Coid
,Attachment.Trail_Id AS MHB_Audit_Trail_Num
,Attachment.Attach_Source AS Detach_Source_Text
,'H' AS SOURCE_SYSTEM_CODE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWCI_STAGING.vwDynamicCareAttachment Attachment
INNER JOIN EDWCI_BASE_VIEWS.REF_MHB_REGIONAL_DATA_CENTER RDC
ON SUBSTR(TRIM(Attachment.DATABASENAME), POSITION('_' IN TRIM(Attachment.DATABASENAME))+1) = RDC.RDC_DESC
INNER JOIN EDWCI_BASE_VIEWS.MHB_USER MU
ON Attachment.Owner_User_Name = MU.USER_LOGIN_NAME 
AND RDC.RDC_SID=MU.RDC_SID
inner join Edw_Pub_Views.Clinical_Facility CF2
on  trim(CF2.Facility_Mnemonic_CS) = trim(Attachment.Patient_FacilityCode)
inner join EDWCDM_BASE_VIEWS.Clinical_Acctkeys CK 
on CK.coid = CF2.coid and  CK.Pat_Acct_Num = Cast (oTranslate(Attachment.Patient_VisitNumber, oTranslate(Attachment.Patient_VisitNumber, '0123456789',''), '') as DECIMAL(18,0))
INNER JOIN EDWCI_BASE_VIEWS.REF_MHB_UNIT RMU2 
ON RMU2.RDC_SID = RDC.RDC_SID
AND Attachment.Created_Unit_Id = RMU2.MHB_UNIT_ID  
INNER JOIN  edwci_base_views.Ref_MHB_Unit RMU1
on RMU1.RDC_Sid = RDC.RDC_SID
AND Attachment.Created_Unit_Id = RMU1.MHB_Unit_Id
LEFT JOIN EDWCI_BASE_VIEWS.REF_MHB_USER_ROLE RMUR
ON TRIM(Attachment.Owner_User_Role)  = TRIM(RMUR.MHB_USER_ROLE_DESC)
AND RMUR.MHB_USER_ROLE_SID = MU.MHB_USER_ROLE_SID
LEFT JOIN EDWCI_BASE_VIEWS.MHB_USER MU2
ON Attachment.CREATED_USER_NAME = MU2.USER_LOGIN_NAME 
AND RDC.RDC_SID=MU2.RDC_SID
LEFT JOIN EDWCI_BASE_VIEWS.REF_MHB_USER_ROLE MUR2 
ON TRIM(Attachment.CREATED_USER_ROLE)  = TRIM(MUR2.MHB_USER_ROLE_DESC)
AND MUR2.MHB_USER_ROLE_SID = MU2.MHB_USER_ROLE_SID
LEFT JOIN Edw_Pub_Views.Clinical_Facility_Location CFL2
on  trim(CFL2.Location_Mnemonic_CS) = trim(SUBSTR(Created_UnitCode,POSITION('_' IN Created_UnitCode)+1))
AND trim(CFL2.Facility_Mnemonic_CS) = trim(Attachment.Created_FacilityCode)
LEFT JOIN Edw_Pub_Views.Clinical_Facility_Location CFL1
on  trim(CFL1.Location_Mnemonic_CS) = trim(SUBSTR(Owner_UnitCode,POSITION('_' IN Owner_UnitCode)+1))
AND trim(CFL1.Facility_Mnemonic_CS) = trim(Attachment.Owner_FacilityCode)
WHERE  CAST(Attach_DateTime AS TIMESTAMP(6)) BETWEEN MU.EFF_FROM_DATE  AND MU.EFF_TO_DATE
AND CAST(Attach_DateTime AS TIMESTAMP(6)) BETWEEN MU2.EFF_FROM_DATE  AND MU2.EFF_TO_DATE
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
) Q