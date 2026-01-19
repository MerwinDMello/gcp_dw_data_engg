WF.sh J_HDW_TMS_Ref_Performance_Rating
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Performance_Rating is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Performance_Rating is Failed."
         exit $RC
fi