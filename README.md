findOutliers from two factors dataset
=====================================

[margusja@vm37 ~]$ pwd
/var/www/html/margusja
[margusja@vm37 ~]$ ls WeroDetectOutliersDemo_v2.jar 
WeroDetectOutliersDemo_v2.jar

# Empty two factors input directory
[hdfs@vm37 ~]$ hdfs dfs -rm -r /user/margusja/input/*

# Generate 1000000 rows of two parameters dataset
[margusja@vm37 ~]$ yarn jar ./WeroDetectOutliersDemo_v2.jar com.nortal.wero.Utils genCSV 1000000 2 10 input/in.csv

# Measure distance from centroid
[margusja@vm37 ~]$ yarn jar ./WeroDetectOutliersDemo_v2.jar com.nortal.wero.findOutliers

# Browse result data - 
http://vm38.dbweb.ee:50075/browseDirectory.jsp?dir=/user/margusja/output2&namenodeInfoPort=50070&nnaddr=213.180.8.135:8020
hdfs dfs -tail /user/margusja/output2/part-r-00000 


find outliers from ten factors dataset
======================================

# Empty input directory
[hdfs@vm37 ~]$ hdfs dfs -rm -r /user/margusja/input10/*

# Generate 1000000 rows of ten parameters dataset
[margusja@vm37 ~]$  yarn jar ./WeroDetectOutliersDemo_v2.jar com.nortal.wero.Utils genCSV 1000000 10 10 input10/in.csv

# Measure distance from centroid with one reducer
[margusja@vm37 ~]$ yarn jar ./WeroDetectOutliersDemo_v2.jar com.nortal.wero.Mahalanobis10 1


# Browse result
http://213.180.8.135:50075/browseDirectory.jsp?dir=/user/margusja/output10&namenodeInfoPort=50070&nnaddr=213.180.8.135:8020
[hdfs@vm37 ~]$ hdfs dfs -tail /user/margusja/output10/part-r-00000

Find outliers using clustering
=============================

#Empty input points diinput points
[hdfs@vm37 ~]$ hdfs dfs -rm -r /user/margusja/points/points

# Generate 1000000 X 10 vectors dataset
yarn jar ./WeroDetectOutliersDemo_v2.jar com.nortal.wero.Utils genVectors 1000000 10 10 points/points

# Calculate two clusters from 10 parameters vectors
yarn jar ./WeroDetectOutliersDemo_v2.jar com.nortal.wero.KMeansClustering 10

# Browse clasters - take last 100 rows
 [margusja@vm37 ~]$ yarn jar ./WeroDetectOutliersDemo_v2.jar com.nortal.wero.Utils readClusters 100
