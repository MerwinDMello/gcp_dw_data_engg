WF.sh J_HDW_TMS_Source_Files_Daily_Extract
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Source_Files_Daily_Extract is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Source_Files_Daily_Extract is Failed."
         exit $RC
fi
