WF.sh J_HDW_TMS_Ref_Employee_Goal_Report
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Employee_Goal_Report is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Employee_Goal_Report is Failed."
         exit $RC
fi