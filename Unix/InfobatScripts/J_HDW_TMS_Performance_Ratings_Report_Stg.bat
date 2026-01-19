WF.sh J_HDW_TMS_Performance_Ratings_Report_Stg
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Performance_Ratings_Report_Stg is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Performance_Ratings_Report_Stg is Failed."
         exit $RC
fi
