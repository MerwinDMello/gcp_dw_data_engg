WF.sh J_HDW_TMS_Ref_Performance_Category
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Performance_Category is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Performance_Category is Failed."
         exit $RC
fi
