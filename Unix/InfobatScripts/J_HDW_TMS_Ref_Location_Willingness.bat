WF.sh J_HDW_TMS_Ref_Location_Willingness
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Location_Willingness is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Location_Willingness is Failed."
         exit $RC
fi
