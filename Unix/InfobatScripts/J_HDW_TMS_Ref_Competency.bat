WF.sh J_HDW_TMS_Ref_Competency
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Competency is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Competency is Failed."
         exit $RC
fi
