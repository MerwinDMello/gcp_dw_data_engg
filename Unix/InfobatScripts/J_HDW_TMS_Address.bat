WF.sh J_HDW_TMS_Address
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Address is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Address is Failed."
         exit $RC
fi
