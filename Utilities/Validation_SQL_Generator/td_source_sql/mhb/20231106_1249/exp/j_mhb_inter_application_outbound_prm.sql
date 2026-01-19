SELECT '$JOBNAME'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM (
SELECT 
RDC.RDC_SID AS RDC_SID,
U10.User_Login_Name AS Outbound_User_Login_Name,
STG.Launch_DateTime AS Outbound_Launch_Date_Time,
coalesce(CK.Patient_DW_Id,999999999999999999) AS Patient_DW_ID,
COALESCE(CK.Company_Code,'H') AS Company_Code,
coalesce(CK.Coid,'99999') AS Coid,
COALESCE(CK.Pat_Acct_Num, 999999999999) AS Pat_Acct_Num,
STG.Trail_ID AS MHB_Audit_Trail_Num,
RMU2.MHB_Unit_Id AS MHB_Unit_Id,
coalesce(CF22.Coid, '99999') AS Outbound_Coid,
coalesce(CFL2.Location_Mnemonic_CS,'Unknown') AS Location_Mnemonic_CS,
U10.MHB_User_Role_Sid AS MHB_User_Role_SID,
STG.InternalDevice_Id AS MHB_Phone_Id,
STG.Launch_App_Name AS Launch_Application_Name,
STG.Launch_Action AS Launch_Action_Name,
STG.Launch_Status AS Launch_Status_Code,
CASE WHEN STG.Launch_SSO_Enabled = 'TRUE' THEN 'Y' WHEN STG.Launch_SSO_Enabled = 'FALSE' THEN 'N' END AS Launch_SSO_Enabled_Ind,
CAST(TRIM(COALESCE(REGEXP_SUBSTR(STG.Launch_Patient_VisitNumber, '[0-9]+'), '999999999999')) AS DECIMAL(12,0)) AS Launch_Pat_Acct_Num,
coalesce(CF31.Coid, '99999') AS Launch_Patient_Coid,
RMU32.MHB_Unit_Id AS Launch_Unit_Id,
coalesce(CFL33.Location_Mnemonic_CS,'Unknown') AS Launch_Location_Mnemonic_CS,
coalesce(CF34.coid, '99999') AS Launch_Coid,
STG.InferredUpdate AS Inferred_Update_Sw,
STG.LastTimeStamp AS MHB_Last_Enter_Date_Time,
'H' AS Source_System_Code,
Current_Timestamp(0) AS DW_Last_Update_Date_Time
from EDWCI_Staging.vwInterAppOutbound STG
INNER JOIN EDWCI_BASE_VIEWS.REF_MHB_REGIONAL_DATA_CENTER RDC
ON SUBSTR(TRIM(STG.DATABASENAME),POSITION('_' IN TRIM(STG.DATABASENAME))+1) = TRIM(RDC.RDC_DESC)
inner join edwci_base_views.MHB_User U10
on stg.User_Name = U10.User_Login_Name
AND RDC.RDC_SID = U10.RDC_SID
--AND U10.Active_DW_Ind = 'Y'
Left join Edwci_Base_Views.Ref_MHB_User_Role R10
on coalesce(trim(stg.User_Role),'Unknown') = trim(R10.MHB_User_Role_Desc)
and R10.MHB_User_Role_Sid = U10.MHB_User_Role_Sid
Left Join Edwcl_Base_Views.Clinical_Facility CF22
on  trim(CF22.Facility_Mnemonic_CS) = trim(stg.FacilityCode)
AND CF22.Facility_Active_Ind = 'Y'
Left join  Edw_Pub_Views.Clinical_Facility CF2
on trim(CF2.Facility_Mnemonic_CS) = trim(stg.Launch_Patient_Facility_Code)
AND CF2.Facility_Active_Ind = 'Y'
Left join EDWCDM_BASE_VIEWS.Clinical_Acctkeys CK 
on CK.coid = CF2.coid
and CK.Pat_Acct_Num = CAST(TRIM(COALESCE(REGEXP_SUBSTR(STG.Launch_Patient_VisitNumber, '[0-9]+'), '999999999999')) AS DECIMAL(12,0))
Inner join edwci_base_views.Ref_MHB_Unit RMU2
on RMU2.RDC_Sid = RDC.RDC_SID
and stg.Unit_Id = RMU2.MHB_Unit_Id
Left Join Edwcl_Base_Views.Clinical_Facility CF31
on  trim(CF31.Facility_Mnemonic_CS) = trim(stg.Launch_Patient_Facility_Code)
AND CF31.Facility_Active_Ind = 'Y'
Left join Edwcl_Base_Views.Clinical_Facility_Location CFL2
on trim(CFL2.Location_Mnemonic_CS) = trim(SUBSTR(stg.UnitCode,POSITION('_' IN UnitCode)+1))
and CF31.COID = CFL2.COID
Inner join edwci_base_views.Ref_MHB_Unit RMU32
on RMU32.RDC_Sid = RDC.RDC_SID
and stg.Launch_Unit_Id = RMU32.MHB_Unit_Id
Left Join Edwcl_Base_Views.Clinical_Facility CF34
on  trim(CF34.Facility_Mnemonic_CS) = trim(stg.Launch_FacilityCode)
AND CF34.Facility_Active_Ind = 'Y'
Left join Edwcl_Base_Views.Clinical_Facility_Location CFL33
on  trim(CFL33.Location_Mnemonic_CS) = trim(SUBSTR(stg.Launch_UnitCode,POSITION('_' IN Launch_UnitCode)+1))
and CF34.COID = CFL33.COID
where cast(stg.Launch_DateTime as date) Between U10.Eff_From_Date and U10.Eff_To_Date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
) A
