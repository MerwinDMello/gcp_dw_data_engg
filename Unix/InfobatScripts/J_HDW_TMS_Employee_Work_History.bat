WF.sh J_HDW_TMS_Employee_Work_History
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Employee_Work_History is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Employee_Work_History is Failed."
         exit $RC
fi