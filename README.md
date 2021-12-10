# Hadoop-MapReduce-Trigram-WordCount
This program counts the occurances of trigrams on any dataset of txt files using the Hadoop MapReduce framework. It also sorts the trigrams by their size in descending order by implementing a Comparator class. A complimentary report is produced with it highlighting all the additional features to optimise the processing of Big Data, such as a FileInputCobiner.
Features includes:
- Google Dataproc optimisation addressed in the report
- Mapper and Reducer (for producing trigrams and counting the number of occurances)
- Intermediate Combiner (optimising the processing through combining the same trigrams from the same mapper and summing up their occurances before reaching the reducer over the network)
- InputTextCombiner (for addressing the hadoop small files problem, by splitting the 10,000 small input files this was tested on into splits of a maximum size of 268MB. Thus, 10,000 mappers aren't created, instead only 15 are)
- Comparotor (for sorting the output in descending order of trigram size)
- 7 Reducers (tested on 2 nodes with 4 processors each therefore 0.95 * 2 * 4 = optimal no. of reducers)
