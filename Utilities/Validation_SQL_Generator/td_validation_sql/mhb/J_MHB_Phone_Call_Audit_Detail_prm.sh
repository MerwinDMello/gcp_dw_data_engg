 #  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Phone_Call_Audit_Detail'


export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Phone_Call_Audit_Detail'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_Phone_Call_Audit_Detail where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT 'J_MHB_Phone_Call_Audit_Detail'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel rdc.rdc_sid 

from
  edwci_staging.vwUserPhoneCalls PC
 inner join Edwci_Base_Views.Ref_MHB_Regional_Data_Center RDC
 on RDC.RDC_Desc =oreplace(PC.databasename,'heartbeatDW_','')
 inner join edwci_base_views.MHB_User MU
on User_Name = User_Login_Name 
Left join Edwci_Base_Views.Ref_MHB_User_Role UR
on trim(User_Role)  = trim(MHB_User_Role_Desc)
and UR.MHB_User_Role_Sid = MU.MHB_User_Role_Sid
Left join 
Edwcl_Base_Views.Clinical_Facility CF
on  trim(CF.Facility_Mnemonic_CS) = trim(PC.FacilityCode)
Left join 
Edwcl_Base_Views.Clinical_Facility_Location CFL
on  trim(CFL.Location_Mnemonic_CS) = trim(SUBSTR(UnitCode,POSITION('_' IN UnitCode)+1))


where  MU.Eff_To_Date = '9999-12-31' (date) and Active_DW_Ind = 'Y'
) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#   