SELECT 'J_CR_RO_Rad_Onc_Procedure_code'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM 
(select
Row_Number() Over (
Order By Cast(dp.DimSiteID as Int) ,Cast(dp.DimProcedureCodeID as Int)) as Procedure_Code_SK,
rtt.Treatment_Type_SK,
rr.Site_SK,
DimProcedureCodeID,
ProcedureCode,
Description,
ProcedureCodeDescription,
ActiveInd,
Log_Id,
Run_Id,
'R' as Source_System_Code,
CURRENT_TIMESTAMP(0) as DW_Last_Update_Date_Time
from
(
	select	
	Cast(DimSiteID as Int) as  DimSiteID,
	Cast(DimProcedureCodeID as Int) as DimProcedureCodeID,
	ProcedureCode,
	Description,
	ProcedureCodeDescription,
	ActiveInd,
	Cast(LogID as Int) as Log_Id,
	Cast(RunID as Int) as Run_Id
	from	EDWCR_Staging.stg_DimProcedureCode) dp
Left outer join (
	Select	distinct Treatment_Type,Treatment_Category,Procedure_Code 
	from	EDWCR_Staging.stg_SC_Modalities ) sm 
	on sm.Procedure_Code=dp.ProcedureCode 
Left Outer join  EDWCR_BASE_VIEWS.Ref_Rad_Onc_Treatment_type rtt 
	on sm.Treatment_Category= rtt.Treatment_Category_Desc 
	and sm.Treatment_Type=rtt.Treatment_Type_Desc
inner join EDWCR_BASE_VIEWS.Ref_Rad_Onc_Site rr 
	on dp.DimSiteID=rr.Source_Site_Id)STG