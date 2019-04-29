## MapReduce  

 - An extnsive tutorial of MapReduce framework covering all Itsfeatures with examples.
[pip](https://pip.pypa.io/en/stable/)


### Topics 

- [Writables](https://github.com/kaustuvkunal/map-reduce/tree/master/src/main/java/com/kk/mapreduce/writables)

- [Combiner](https://github.com/kaustuvkunal/map-reduce/blob/master/src/main/java/com/kk/mapreduce/wordcount/WCDriver.java ) 
  > is mini reduce for a single Map task. It optimises the processing by minimising the amount of data being flown from one node to another. Use it if reducer operation is commutative and associative Specify combiner in driver as job.setCombinerClass(SomeReducer.class); Combiner's Input and output key & value type are same.
  
-  [Distributed Cache](https://github.com/kaustuvkunal/map-reduce/tree/master/src/main/java/com/kk/mapreduce/distributedcache)
 > Distributed cache provide service to copy side data to the task node. Files are copied to node one per job. File path is specified in driver class as : Job.addCacheArchive(URI) , use for larger side data
 
- [Partitioner](https://github.com/kaustuvkunal/map-reduce/tree/master/src/main/java/com/kk/mapreduce/partitioner) 
  > Partitioner class partitions the keys of intermediate map output and ensure identical keys go to same reducer
 
- [Custom Partitioner and Sorting using Custom Partitioner](https://github.com/kaustuvkunal/map-reduce/blob/master/src/main/java/com/kk/mapreduce/partitioner/MyCustomPartitioner.java)
 
- [Total-Order-Partitioner and sorting using Total-Order-Partitioner](https://github.com/kaustuvkunal/map-reduce/tree/master/src/main/java/com/kk/mapreduce/totalordersort)

- [Solving Top N Problem using MapReduce](https://github.com/kaustuvkunal/map-reduce/tree/master/src/main/java/com/kk/mapreduce/topnproblem)

- [Finding facebook Common Friend using MapReduce](https://github.com/kaustuvkunal/map-reduce/tree/master/src/main/java/com/kk/mapreduce/commonfriends)

- [Secondary Sort](https://github.com/kaustuvkunal/map-reduce/tree/master/src/main/java/com/kk/mapreduce/secondarysort)

- [Processing large number of small file using CombineFileInputFormat & Custom Writable](https://github.com/kaustuvkunal/map-reduce/tree/master/src/main/java/com/kk/mapreduce/maxtempusingcombineinputformat)

- Counters
 > Counter are facility for MapReduce applications to report its statistics,it is useful in problem diagnosis and validation
 
- [Compression](https://github.com/kaustuvkunal/map-reduce/tree/master/src/main/java/com/kk/mapreduce/maxtemp)


- [Test Using MR-Unit](https://github.com/kaustuvkunal/map-reduce/tree/master/src/test/java/com/kk/test/mapreduce


####  Pre-requisite

 -  Hadoop version 2.7.5 (psudo-distribution or distribution mode)
 </br> In case of diffrent version update the pom s : haddop version 

####  Execute
</br>`hadoop jar <jar-path> <driver-class-path> <hdfs-input-path> <hdfs-output-path>`

- All example are tested with  Apache hadoop (2.7.5) psudodistribution mode


###### Please feel free to drop a message if you find any mapreduce feature is missing. I will include.
 
