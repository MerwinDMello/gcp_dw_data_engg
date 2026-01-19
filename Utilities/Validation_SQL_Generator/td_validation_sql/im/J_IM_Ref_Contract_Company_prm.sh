export Job_Name='J_IM_Ref_Contract_Company'
export JOBNAME='J_IM_Ref_Contract_Company'
export col_name='IM_Contract_Company_SID'
export tab_name='EDWIM.Ref_IM_Contract_Company'


export AC_EXP_SQL_STATEMENT="select 'J_IM_Ref_Contract_Company' + ',' +
coalesce(cast(ltrim(rtrim(B.counts)) as varchar(20)),'0')+ ','  AS SOURCE_STRING FROM  
(
select  count(*) as counts from 
(select  distinct [corpadNet2001-CORPds-ContractCompany] as contractcompany,[corpadNet2001-CORPds-ContractEmail] as contractemail,
[corpadNet2001-CORPds-ContractFax] as contractfax,[corpadNet2001-CORPds-ContractPhone] as contractphone
from  IDMart.dbo.ActiveDirectoryUsers
WHERE NOT([corpadNet2001-CORPds-ContractCompany] IS NULL AND [corpadNet2001-CORPds-ContractEmail] IS NULL AND [corpadNet2001-CORPds-ContractFax] IS NULL AND [corpadNet2001-CORPds-ContractPhone] IS NULL)
AND NOT([corpadNet2001-CORPds-ContractCompany] = '' AND [corpadNet2001-CORPds-ContractEmail] = '' AND [corpadNet2001-CORPds-ContractFax] = '' AND [corpadNet2001-CORPds-ContractPhone] = '')) A
) B;" 


export AC_ACT_SQL_STATEMENT="select 'J_IM_Ref_Contract_Company' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM.Ref_IM_Contract_Company                
)A;"

