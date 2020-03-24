import json
import botocore

def load_json_from_s3(bucket, key):

    print("event key " + key)
    # strip off directory from key for temp file
    key_split = key.split('/')
    download_fn=key_split[len(key_split) - 1]
    file = "/tmp/" + download_fn
    
    try:
        bucket.download_file(key, file)
    except botocore.exceptions.ClientError as e:
        print("Error reading the s3 object " + key)
        jsonData = {"message": "error"}
        return jsonData

    try:
        with open(file) as f:
            jsonData = json.load(f)
        f.close()
    except IOError:
        print("Could not read file:" + file)
        jsonData = {"message": "error"}

    return jsonData

def update_status_on_s3(bucket, request_id, type, status, message):
    statusJson = {"request_id": request_id, "type": type, "status": status, "message": message}
    with open("/tmp/" + request_id + "_"+ type +".json", 'w') as status_file:
        json.dump(statusJson, status_file)
    #        json.dump(districtPrecipStats, json_file)
    status_file.close()

#    bucket.upload_file("/tmp/" + request_id + "_" + type +".json",
#                                       "status/" + request_id + "_" + type +".json")
    bucket.upload_file("/tmp/" + request_id + "_" + type +".json",
                                       "status/" + request_id + ".json")
