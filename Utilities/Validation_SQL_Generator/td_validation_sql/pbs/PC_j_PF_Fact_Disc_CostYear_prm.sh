#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMPC300'
export PE_DATE='9999-12-31' 


export AC_EXP_SQL_STATEMENT="Select 'PBMPC300' ||',' || cast(A.Row_Count as varchar(20)) ||',' as Source_String from (Select count(*) as Row_Count From Edwpf_Base_Views.Dim_Organization DOrg Left outer join Edwfs_Base_Views.Facility FAC on FAC.Unit_Num = DOrg.Org_Sid Where substr(DOrg.Org_Name_Child,1,1) = 'Z' AND Dorg.Org_Hier_Name like 'AR Org Hier%') A"
#"Select 'PBMPC300' ||',' || cast(A.Row_Count as varchar(20)) ||',' as Source_String from (Select count(*) as Row_Count From Edwpbs_staging.Dim_Organization DOrg Left outer join Edwfs.Facility FAC on FAC.Unit_Num = DOrg.Org_Sid ,Edwpf_Staging.EIS_Dcrp_Unit_SourceYear_Dim USY Where Substring(DOrg.Org_Name_Child,1,1) = 'Z' AND Dorg.Org_Hier_Name like 'AR Org Hier%') A"
#export AC_EXP_SQL_STATEMENT="Select 'PBMPC300' ||',' || cast(A.Row_Count as varchar(20)) ||',' as Source_String from (Select count(*) as Row_Count From Edwpbs_staging.Dim_Organization DOrg Left outer join Edwfs.Facility FAC on FAC.Unit_Num = DOrg.Org_Sid  Where Substring(DOrg.Org_Name_Child,1,1) = 'Z' AND Dorg.Org_Hier_Name like 'AR Org Hier%') A"
export AC_ACT_SQL_STATEMENT="Select 'PBMPC300' ||',' || cast(A.Row_Count as varchar(20)) ||',' as Source_String from (Select count(*) as Row_Count from Edwpbs_staging.EIS_Dcrp_Unit_Cost_Year_Dim) A"

#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#  
