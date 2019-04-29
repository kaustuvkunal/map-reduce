
- Read only data needed by a job in order to process main data is known as side data

- MapReduce job accesses side data by below two ways,
 
  1. Job Configuration :  It serializes the data, put all the data inside memory and accessed using context’s get configuration method.  Use it when side data size is in under few kilobytes
 
  2. Distributed cache :   Distributed cache provide service to copy side data to the task node. Files are copied to node one per job. File path is specified in driver class  as  : Job.addCacheArchive(URI) , use for larger side data
