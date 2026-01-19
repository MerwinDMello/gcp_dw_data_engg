export Job_Name='J_IM_Ref_IM_Domain'
export JOBNAME=''J_IM_Ref_IM_Domain''
export col_name='IM_Domain_Id'
export tab_name='EDWIM.Ref_IM_Domain'

export AC_EXP_SQL_STATEMENT="select 'J_IM_Ref_IM_Domain' + ',' +
coalesce(cast(ltrim(rtrim(A.counts)) as varchar(20)),'0')+ ','  AS SOURCE_STRING FROM  
(SELECT
	
count( distinct DomainName) as counts from 
		[IDMart].[dbo].[ActiveDirectoryUsers] where coalesce(DomainName,'')<> ''
	) a
	
	;" 

export AC_ACT_SQL_STATEMENT=" select 'J_IM_Ref_IM_Domain' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM.Ref_IM_Domain   where source_system_code='A'             

           
           )A;;"


