#
# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
#      http://aws.amazon.com/asl/                                                                           
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions
# and limitations under the License.        
#

import os
import sys
import requests
from pyspark.sql import SparkSession

def connect():
    END_POINT ='https://q652q8ycce.execute-api.us-east-2.amazonaws.com/dev'
    
    get_response = requests.get(END_POINT)
    
    print(get_response)
    print(get_response.content)
    
    post_response = requests.post(END_POINT)
    
    print(post_response)
    print(post_response.content)
    
if __name__ == "__main__":
    """
        Usage: wordcount [destination path]
    """
    spark = SparkSession\
        .builder\
        .appName("WordCount")\
        .getOrCreate()

    output_path = None

    if len(sys.argv) > 1:
        output_path = sys.argv[1]
    else:
        print("Job failed. Please provide destination bucket path using entryPointArguments parameter.")
        sys.exit(1)

    region = os.getenv("AWS_REGION")
    text_file = spark.sparkContext.textFile("s3://" + region  + ".khanh-bucket/wordcount/input-small")
    
    
    text_file.flatMap(lambda line: line.split(" "))
            .foreach(connect)


    spark.stop()
