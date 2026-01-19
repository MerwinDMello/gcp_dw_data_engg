Collect Stored Procedures from Teradata, and store them as .SQL files.
-
<br></br>

Configuration
-
1. You will need to change the 3-4ID in the script, and you will want to adjust your Teradata credentials for the script to run properly. 

2. Once you have made these adjustments, make sure to organize your .csv file with all of your Stored Procedures in a single column without a header. Specifically place them in the first column. The script uses this list to loop through all the procedures you need to collect.

<br></br>
Running the script
- 

1. Execute the python script
2. Outputs will be stored in the same folder (I plan on adding an output folder in the future)
