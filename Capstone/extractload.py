import boto3

s3_resource = boto3.resource('s3')

new_bucket_name = "s3://staging94"
bucket_to_copy = "sourceBucketName"

for key in s3.list_objects(Bucket=bucket_to_copy)['Contents']:
    files = key['Key']
    copy_source = {'Bucket': "bucket_to_copy",'Key': files}
    s3_resource.meta.client.copy(copy_source, new_bucket_name, files)
    print(files)
    