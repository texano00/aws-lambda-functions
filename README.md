## AWS lambda functions


### python2.7/:

-   **kinesis_stream_put_to_s3_athena_partitioning.py** 
    
    Description: process data from kinesis stream and put to s3, partitioning the data for Athena. The partition is based on       the timestamp of the "d" value, so is not important the order of the stream data.
  	
  	Environment Variables: 
  		S3_BUCKET -> name of the s3 bucket (es. 'bucket')
  		S3_PATH -> custom path (es. 'test/test1/test2')

  	Example minimum json:
  	{
  		"_id":"asdasdasdasd",
  		"d":{"sec":1498746471}
  	}
    
  
