#  @@START DMEXPRESS_EXPORTED_VARIABLES

. /etl/ST/PBS/PA/LOGON/PA_DB.sh

#export FM_Activity_Date=2011-03-04

export PE_DATE='9999-12-31'
export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMPA100-10'
	export AC_ACT_SQL_STATEMENT="SELECT 'PBMPA100-10'||','||CAST (SUM(ADA_Pct) AS varchar(20)) ||','||CAST (SUM(Secondary_Pct) AS varchar(20))||',' 
	as source_string FROM Edwpbs_base_views.Ref_ADA_Facility_Metric WHERE	Eff_To_Date = '9999-12-31' AND unit_num NOT IN ('27102','00631','00460','00472' ) "

export AC_EXP_SQL_STATEMENT="SELECT 'PBMPA100-10' + ',' + Isnull(cast(Sum(ADAPct) as varchar(20)),'0') + ',' + Isnull(cast(Sum(SecondaryPct) as varchar(20)),'0')+',' 
as source_string FROM ARApplication.ADA.Facility"

#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  