## AWS lambda functions


### python2.7/:

-   Name: **kinesis_stream_put_to_s3_athena_partitioning.py** 
    
    Description: process data from kinesis stream and put to s3, partitioning the data for Athena
  	
  	Environment Variables: 
  		S3_BUCKET -> name of the s3 bucket (es. 'bucket')
  		S3_PATH -> custom path (es. 'test')

  	Example minimum data:
  	{
  		"_id":"asdasdasdasd",
  		"d":{"sec":1498746471}
  	}