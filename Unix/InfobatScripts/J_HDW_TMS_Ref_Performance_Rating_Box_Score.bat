WF.sh J_HDW_TMS_Ref_Performance_Rating_Box_Score
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Performance_Rating_Box_Score is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Performance_Rating_Box_Score is Failed."
         exit $RC
fi