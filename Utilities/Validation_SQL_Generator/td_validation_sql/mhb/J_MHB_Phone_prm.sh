 #  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_MHB_Phone'


export AC_ACT_SQL_STATEMENT="SELECT 'J_MHB_Phone'||','||CAST(Count(*) AS VARCHAR (20))||','

AS SOURCE_STRING FROM
(
sel * from Edwci.MHB_Phone  where dw_last_update_date_time(date)=current_date
 ) Q"

export AC_EXP_SQL_STATEMENT="SELECT 'J_MHB_Phone'||','||CAST(Count(*) AS VARCHAR (20))||','
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
 
) Q"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#    