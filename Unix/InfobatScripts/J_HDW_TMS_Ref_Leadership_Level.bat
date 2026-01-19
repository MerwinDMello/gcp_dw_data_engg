WF.sh J_HDW_TMS_Ref_Leadership_Level
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Leadership_Level is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Leadership_Level is Failed."
         exit $RC
fi