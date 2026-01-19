WF.sh J_HDW_TMS_Employee_Goal_Detail
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Employee_Goal_Detail is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Employee_Goal_Detail is Failed."
         exit $RC
fi
