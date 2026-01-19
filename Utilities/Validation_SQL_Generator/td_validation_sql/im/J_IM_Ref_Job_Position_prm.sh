export Job_Name='J_IM_Ref_Job_Position'
export JOBNAME='J_IM_Ref_Job_Position'
export col_name='IM_Position_Sid'
export tab_name='EDWIM.Ref_IM_Job_Position'


export AC_EXP_SQL_STATEMENT="select 'J_IM_Ref_Job_Position' + ',' +
coalesce(cast(ltrim(rtrim(B.counts)) as varchar(20)),'0')+ ','  AS SOURCE_STRING FROM  
(
select  count(*) as counts from 
(select distinct  [corpadNet2001-CORPds-PositionCode] ,[corpadNet2001-CORPds-PositionCodeDescription] from  IDMart.dbo.ActiveDirectoryUsers
WHERE NOT([corpadNet2001-CORPds-PositionCode] IS NULL AND [corpadNet2001-CORPds-PositionCodeDescription] IS NULL)
AND NOT([corpadNet2001-CORPds-PositionCode] = '' AND [corpadNet2001-CORPds-PositionCodeDescription] = '')) A
) B;" 


export AC_ACT_SQL_STATEMENT="select 'J_IM_Ref_Job_Code' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM.Ref_IM_Job_Position                
)A;"

