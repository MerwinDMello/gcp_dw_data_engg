 #  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Ref_MHB_Login_Type'


export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Ref_MHB_Login_Type'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from Edwci.Ref_MHB_Login_Type where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT 'J_MHB_Ref_MHB_Login_Type'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from (sel distinct Login_Type 
 from EDWCI_Staging.vwUserLogins)X
 where X.Login_Type not in (sel Login_Type_Desc from Edwci.Ref_MHB_Login_Type  )
) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#    