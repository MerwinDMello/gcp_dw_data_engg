WF.sh J_HDW_TMS_Ref_Performance_Plan
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Performance_Plan is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Performance_Plan is Failed."
         exit $RC
fi
