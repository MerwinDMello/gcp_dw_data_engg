export Job_Name='J_IM_Ref_Job_Code'
export JOBNAME='J_IM_Ref_Job_Code'
export col_name='IM_Job_Code_SId'
export tab_name='EDWIM.Ref_IM_Job_Code'


export AC_EXP_SQL_STATEMENT="select 'J_IM_Ref_Job_Code' + ',' +
coalesce(cast(ltrim(rtrim(B.counts)) as varchar(20)),'0')+ ','  AS SOURCE_STRING FROM  
(
select  count(*) as counts from 
(select  distinct [corpadNet2001-CORPds-JobCode],[corpadNet2001-CORPds-JobCodeDescription] from  IDMart.dbo.ActiveDirectoryUsers
WHERE NOT([corpadNet2001-CORPds-JobCode] IS NULL AND [corpadNet2001-CORPds-JobCodeDescription] IS NULL)
AND NOT([corpadNet2001-CORPds-JobCode] = '' AND [corpadNet2001-CORPds-JobCodeDescription] = '')) A
) B;" 


export AC_ACT_SQL_STATEMENT="select 'J_IM_Ref_Job_Code' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM.Ref_IM_Job_Code                
)A;"

