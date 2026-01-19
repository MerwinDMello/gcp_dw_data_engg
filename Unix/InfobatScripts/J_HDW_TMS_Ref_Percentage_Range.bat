WF.sh J_HDW_TMS_Ref_Percentage_Range
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Percentage_Range is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Percentage_Range is Failed."
         exit $RC
fi