#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Alerting_Audit_Detail'


export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Alerting_Audit_Detail'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_ALERTING_AUDIT_DETAIL where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT 'J_MHB_Alerting_Audit_Detail'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel
RDC_SID                       

from
(select
RDC.RDC_SID 

FROM EDWCI_STAGING.vwPatientAlertTracker A
INNER JOIN EDWCI_BASE_VIEWS.REF_MHB_REGIONAL_DATA_CENTER RDC
ON OREPLACE(TRIM(A.DATABASENAME),'heartbeatDW_','')  = TRIM(RDC.RDC_DESC)
--ON A.DATABASENAME = TRIM(RDC.RDC_DESC)
 Left join Edwci_Base_Views.Ref_MHB_Alert_Type 
 on Coalesce(Trim(Alert_Title), 'Unknown') = Ref_MHB_Alert_Type.Alert_Type_Desc
inner join  edwci_base_views.MHB_User USR
on A.User_Name = USR.User_Login_Name 
and RDC.RDC_SID=USR.RDC_SID
Left join Edwci_Base_Views.Ref_MHB_User_Role URL
on trim(User_Role)= trim(MHB_User_Role_Desc)
and URL.MHB_User_Role_Sid = USR.MHB_User_Role_Sid
LEFT JOIN EDWCL_BASE_VIEWS.CLINICAL_FACILITY CF1
ON  TRIM(CF1.FACILITY_MNEMONIC_CS) = TRIM(A.FacilityCode )
LEFT JOIN EDWCL_BASE_VIEWS.CLINICAL_FACILITY CF2
ON  TRIM(CF2.FACILITY_MNEMONIC_CS) = TRIM(A.Patient_Facility_Code)
--LEFT JOIN EDWPF_STAGING.CLINICAL_ACCTKEYS CK 
--ON CK.COID = CF2.COID 
--AND  CK.PAT_ACCT_NUM=(CASE WHEN OTRANSLATE(A.PATIENT_VISITNUMBER, OTRANSLATE(A.PATIENT_VISITNUMBER, '0123456789',''), '')='' THEN 0 ELSE CAST(OTRANSLATE(A.PATIENT_VISITNUMBER, OTRANSLATE(A.PATIENT_VISITNUMBER, '0123456789',''), '') AS DECIMAL(12,0)) END) 
 Left join Edwcl_Base_Views.Clinical_Facility_Location CFL
on  trim(CFL.Location_Mnemonic_CS) = trim(SUBSTR(UnitCode,POSITION('_' IN UnitCode)+1))
where  USR.Eff_To_Date = date '9999-12-31' and Active_DW_Ind = 'Y' ) C
) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#    