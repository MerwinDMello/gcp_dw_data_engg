SELECT 'J_EP_Junc_Remittance_Delete' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' as Source_String from
(
Select 
distinct 
pmt.Check_Num_AN as Check_Num_An
,pmt.Check_Date as Check_Date
,pmt.Check_Amt as Check_Amt
,pmt.Interchange_Sender_Id as Interchange_Sender_Id
,pmt.Provider_Level_Adj_Id as Provider_Adjustment_Id
,clm.Payment_GUID as Payment_GUID
,clm.Claim_GUID as Claim_GUID
,svc.Service_Guid as Service_GUID
,clm.Delete_Date as Delete_Date
,pmt.Coid as Coid
,pmt.Unit_Num as Unit_Num
,clm.Company_Code as Company_Code
,'E' as Source_System_Code
,current_timestamp(0) as DW_Last_Update_Date_Time
From edwpbs_base_views.remittance_claim clm
join edwpbs_base_views.remittance_payment pmt
on clm.payment_guid =pmt.Payment_GUID
join edwpbs_base_Views.remittance_Service svc
on svc.claim_guid =clm.claim_guid
)a
