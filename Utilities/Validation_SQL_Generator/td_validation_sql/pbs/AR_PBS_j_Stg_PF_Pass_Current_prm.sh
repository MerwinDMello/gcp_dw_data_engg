#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMAR095'



export AC_EXP_SQL_STATEMENT="SELECT 'PBMAR095' || ',' || cast(zeroifnull(count(*)) as varchar(20))  || ','  AS SOURCE_STRING
FROM Edwpf_Staging.Pass_Current where rptg_Period = cast ( ( add_months(current_Date,-1) (format 'YYYYMM')) as Char(6))"


export AC_ACT_SQL_STATEMENT="SELECT 
'PBMAR095' || ',' || cast(zeroifnull(count(*)) as varchar(20)) || ','   AS SOURCE_STRING
FROM Edwpbs_Staging.Pass_Current_PF PC where rptg_Period = cast ( ( add_months(current_Date,-1) (format 'YYYYMM')) as Char(6)) 
and DW_Last_Update_Date_Time in 
(select max (DW_Last_Update_Date_Time) from Edwpbs_Staging.Pass_Current_PF)"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  

