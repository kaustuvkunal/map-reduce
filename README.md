## MapReduce  

 - An extnsive tutorial of MapReduce framework covering all the features of mapreduce with examples.
[pip](https://pip.pypa.io/en/stable/)

### Topics 

- [Combiner](https://github.com/kaustuvkunal/map-reduce/blob/master/src/main/java/com/kk/mapreduce/wordcount/WCDriver.java ) :
  > is mini reduce for a single Map task. It optimises the processing by minimising the amount of data being flown from one node to another. Use it if reducer operation is commutative and associative Specify combiner in driver as job.setCombinerClass(SomeReducer.class); Combiner's Input and output key & value type are same.
  
- Distributed Cache
- Custom Partitioner and Sorting using Custom Partitioner
- Total-Order-Partitioner and sorting using Total-Order-Partitioner
- Solving Top N Problem using MapReduce 
- Finding facebook Common Friend using MapReduce
- Secondary Sort
- Processing large number of small file using CombineFileInputFormat & Custom Writable
- Counters
- Test Using MR-Unit


####  Pre-requisite

 -  Hadoop version 2.7.5 (psudo-distribution or distribution mode)
 </br> In case of diffrent version update the pom s : haddop version 

####  Execute
</br>`hadoop jar <jar-path> <driver-class-path> <hdfs-input-path> <hdfs-output-path>`

- All example are tested with  Apache hadoop (2.7.5) psudodistribution mode


###### Please feel free to drop a message if you find any mapreduce feature is missing. I will include.
 
