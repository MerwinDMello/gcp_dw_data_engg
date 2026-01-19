SELECT 'J_MHB_User'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel 
RDC_SID  
    
from   Edwci_staging.MHB_User_wrk
where (
coalesce(RDC_SID,0)  ,                     
coalesce(User_Login_Name,'')               ,
coalesce(MHB_User_Role_SID,0)     ,
--coalesce(coid,'')               ,
--coalesce(Company_Code,'')               ,
coalesce(User_Title_Text,'')         ,      
coalesce(User_First_Name,'')        ,       
coalesce(User_Last_Name,'')         ,       
coalesce(User_Full_Name,'')           ,     
coalesce(User_Email_Text,'')      ,
coalesce(External_Phone_1_Num_Label_Text,''),
coalesce(External_Phone_1_Num,'')          ,
coalesce(External_Phone_2_Num_Label_Text,''),
coalesce(External_Phone_2_Num,'')          ,
coalesce(External_Phone_3_Num_Label_Text,''),
coalesce(External_Phone_3_Num,'')          ,
coalesce(External_Phone_4_Num_Label_Text,''),
coalesce(External_Phone_4_Num,'')    ,
coalesce(SIP_Num,'')                       ,
coalesce(Internal_User_Ind,'')           ,  
coalesce(Active_DW_Ind,0)               ,  
coalesce(MHB_Audit_Trail_Num,0)    )
not in (sel coalesce(RDC_SID,0)  ,                     
coalesce(User_Login_Name,'')               ,
coalesce(MHB_User_Role_SID,0)     ,
--coalesce(coid,'')               ,
--coalesce(Company_Code,'')               ,
coalesce(User_Title_Text,'')         ,      
coalesce(User_First_Name,'')        ,       
coalesce(User_Last_Name,'')         ,       
coalesce(User_Full_Name,'')           ,     
coalesce(User_Email_Text,'')      ,
coalesce(External_Phone_1_Num_Label_Text,''),
coalesce(External_Phone_1_Num,'')          ,
coalesce(External_Phone_2_Num_Label_Text,''),
coalesce(External_Phone_2_Num,'')          ,
coalesce(External_Phone_3_Num_Label_Text,''),
coalesce(External_Phone_3_Num,'')          ,
coalesce(External_Phone_4_Num_Label_Text,''),
coalesce(External_Phone_4_Num,'')    ,
coalesce(SIP_Num,'')                       ,
coalesce(Internal_User_Ind,'')           ,  
coalesce(Active_DW_Ind,0)               ,  
coalesce(MHB_Audit_Trail_Num,0)    
    from  Edwci.MHB_User
	
	)
 
) Q