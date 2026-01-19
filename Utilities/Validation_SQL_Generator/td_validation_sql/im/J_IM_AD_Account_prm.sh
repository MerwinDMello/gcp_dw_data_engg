export Job_Name='J_IM_AD_Account'
export JOBNAME='J_IM_AD_Account'


export AC_EXP_SQL_STATEMENT="select 'J_IM_AS_Account' + ',' +
coalesce(cast(ltrim(rtrim(A.counts)) as varchar(20)),'0')+ ','  AS SOURCE_STRING FROM  
(select
count(*) as counts
from  dbo.ActiveDirectoryUsers
WHERE DomainName = 'HCA.Corpad.net'
AND Is34UID = 1
AND NOT(UserName LIKE 'ADT%')
AND LEN(ltrim(rtrim(UserName))) =7
	) a;" 

export AC_ACT_SQL_STATEMENT="select 'J_IM_AD_Account' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM_staging.AD_Account_Stg                
)A;"