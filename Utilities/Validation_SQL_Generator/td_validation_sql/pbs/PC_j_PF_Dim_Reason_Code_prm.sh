#  @@START DMEXPRESS_EXPORTED_VARIABLES


export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PCMEI280'

export AC_ACT_SQL_STATEMENT="SELECT 'PCMEI280'||','|| coalesce(trim(CAST(COUNT(Reason_Code_Sid) as varchar(20))),'0')||','as Source_String FROM EDWPBS.EIS_Reason_Code_Dim "

 

export AC_EXP_SQL_STATEMENT="SELECT 'PCMEI280'||','|| coalesce(trim(CAST(sum(K.Row_Count)as varchar(20))),'0')||',' as Source_String FROM 
 (
select count(*)  as Row_Count
FROM
 EDWPBS.Ref_Discrepancy_Reason
where ('RC_'||Discrepancy_Reason_Code)
not in (select reason_code_member from EDWPBS.EIS_Reason_Code_Dim)
union
select count(*)  from EDWPBS.EIS_Reason_Code_Dim) K"
