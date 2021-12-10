# Hadoop-MapReduce-Trigram-WordCount
BigData handling with Hadoop framework and Google Dataproc. This program counts the occurances of trigrams on any dataset of txt files using the Hadoop MapReduce framework. It also sorts the trigrams by their size in descending order by implementing a Comparator class. A complimentary report is produced with it highlighting all the additional features to optimise the processing of Big Data, such as a FileInputCobiner.
Features includes:
- Google Dataproc optimisation addressed in the report
- Mapper and Reducer (for producing trigrams and counting the number of occurances)
- Intermediate Combiner (optimising the processing through combining the same trigrams from the same mapper and summing up their occurances before reaching the reducer over the network)
- InputTextCombiner (for addressing the hadoop small files problem, by splitting the 10,000 small input files this was tested on into splits of a maximum size of 268MB. Thus, 10,000 mappers aren't created, instead only 15 are)
- Comparotor (for sorting the output in descending order of trigram size)
- 7 Reducers (tested on 2 nodes with 4 processors each therefore 0.95 * 2 * 4 = optimal no. of reducers)

# Installation and Running the Program with Google DataProc:
1. Download the wc.jar file in this repository
2. Create a Google Storage Bucket and upload the wc.jar file to the bucket
3. Upload your input file(s) to your bucket too into an input folder
4. Enable the Compute Engine API in GCP
5. Create a Google Dataproc cluster and ensure that its location is the same as the location of the GCP bucket. 
6. Open Google cloudshell and run the following command with the name and region of your cluster and the path of your input and output repositories in your bucket:
  <i> gcloud dataproc jobs submit hadoop --cluster [yourClustersName] --region=[yourClustersRegion] --jar gs://[yourBucketsName]/wc.jar -- WordCount gs://[yourBucketsName]/input gs://[yourBucketsName]/output </i>
7. Once the map and reduce reach 100%, an output folder should be created in your bucket. Open the files in this folder to see the outputs of this program.  
