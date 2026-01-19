WF.sh J_HDW_TMS_Fact_Employee_Availability
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Fact_Employee_Availability is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Fact_Employee_Availability is Failed."
         exit $RC
fi
