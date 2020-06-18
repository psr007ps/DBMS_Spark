DBMS Project
Submitted By: Pranshu Shrivastava


Execution Instructions
Main code file: dbms_project.py
Shell file: run.sh


Skeleton command:
bash run.sh /path/to/WeatherStationLocations.csv /path/to/recordings /path/of/output/file 


An overall description of how you chose to divide the problem into different spark operations, with reasoning.
The entire project has been divided into following steps:
1. In the first step, the csv file is read to gather all the records in the United States grouped by states.
2. The second step is divided into sub-parts:
   1. Data from the recordings files is taken into an RDD.
   2. The value of precipitation is parsed to obtain the end character and the calculation is done to identify its value. A mapper is used to transform the RDD.
   3. The next step is to calculate the average value of precipitation for each month.
3. Third step is to find the months with the highest and lowest averages for each state. 
4. Lastly, order the states by the difference between the highest and lowest month average, in ascending order. The result is finally stored in a csv file.


A description of each step job
1. Google Colab and pyspark are used for the development of this project.
2. A timer is put to record the start and end times for running the code.
3. To achieve task1, a filter function is used to filter the records with country = US. The data thus obtained is grouped by states using a groupBy statement.
4. To achieve task2,
   1. An RDD is created with the data from the recordings file.
   2. A function is described to calculate the value of precipitation using the character’s value appended at the end.
   3. A mapper is used to call the function described in 4.b. This creates a new RDD of the same dimensions after doing the transformations described in the function.
   4. A dataframe is created with the obtained precipitation values.
   5. In the next step, month is extracted from the date.
   6. The dataframe is then grouped by state and month, and aggregated using average value. Another column is added having the aggregated value.
5. To achieve task3,
   1. Two new dataframes are created for storing the maximum and minimum value for each state.
   2. Two more dataframes are created which compares the dataframes created for 4.e and 5.a. They store the maximum and minimum value of average precipitation.
   3. The dataframes from 4.e and 5.a are joined to get maximum and minimum precipitation for each state.
   4. Dataframes are finally joined in a way that they give the months with the highest and lowest averages for each state. 
6. To achieve task4, create a new column in the dataframe to store the difference between the maximum and minimum temperature. Save the records in ascending order of the difference. Finally, the coalesce method is used to write the data in a output file named 'Result.csv'


Total time taken to run the code:
Time taken = 251.5 seconds.