WF.sh J_HDW_TMS_Employee_Talent_Profile
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Employee_Talent_Profile is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Employee_Talent_Profile is Failed."
         exit $RC
fi
