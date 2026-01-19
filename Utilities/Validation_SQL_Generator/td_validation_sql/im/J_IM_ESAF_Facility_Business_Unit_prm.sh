export Job_Name='J_IM_ESAF_Facility_Business_Unit'
export JOBNAME='J_IM_ESAF_Facility_Business_Unit'


export AC_EXP_SQL_STATEMENT="select 'J_IM_ESAF_Facility_Business_Unit' + ',' +
coalesce(cast(ltrim(rtrim(A.counts)) as varchar(20)),'0')+ ','  AS SOURCE_STRING FROM  
(select
count(*) as counts
from  IDMart.dbo.FacilityBusinessUnit
	) A;" 

export AC_ACT_SQL_STATEMENT="select 'J_IM_ESAF_Facility_Business_Unit' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' as source_string from 
(
SELECT
	COUNT(*) as counts
	FROM EDWIM.eSAF_Facility_Business_Unit                
)A;"