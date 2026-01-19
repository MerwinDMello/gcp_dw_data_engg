WF.sh J_HDW_TMS_Employee_Development_Activity
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Employee_Development_Activity is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Employee_Development_Activity is Failed."
         exit $RC
fi
