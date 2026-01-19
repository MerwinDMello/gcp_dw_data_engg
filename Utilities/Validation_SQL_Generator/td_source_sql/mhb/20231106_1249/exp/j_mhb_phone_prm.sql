SELECT 'J_MHB_Phone'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel 
RDC_SID                   
  
from   Edwci_staging.MHB_Phone_wrk
where (
coalesce(RDC_SID,0)  ,                     
coalesce(MHB_Phone_Id,'')               ,
coalesce(Phone_Id_Text,'')         ,      
coalesce(Phone_Name,'')        ,       
coalesce(Phone_OS_Code,'')         ,       
coalesce(OS_Version_Text,'')           ,     
coalesce(Device_Pooling_Config_Code,'')      ,
coalesce(Active_DW_Ind,'')
    )
not in (sel coalesce(RDC_SID,0)  ,                     
coalesce(MHB_Phone_Id,'')               ,
coalesce(Phone_Id_Text,'')         ,      
coalesce(Phone_Name,'')        ,       
coalesce(Phone_OS_Code,'')         ,       
coalesce(OS_Version_Text,'')           ,     
coalesce(Device_Pooling_Config_Code,'')      ,
coalesce(Active_DW_Ind,'')  
    from  Edwci.MHB_Phone)
 
) Q