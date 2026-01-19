WF.sh J_HDW_TMS_Ref_Future_Role_Attribute
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Future_Role_Attribute is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Future_Role_Attribute is Failed."
         exit $RC
fi