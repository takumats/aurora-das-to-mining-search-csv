# aurora-das-to-mining-search-csv

A Python script to convert a Database Activity Streams (DAS) output to "MiningSearch" style CSV file.

## usage
DAS output json file to "MiningSearch" style CSV file.
```
python3 aumy_das_json_to_mscsv.py <AUMY_DAS_JSON> <OUTPUT_CSV_FILE_NAME> 
```

Get DAS data as json text from Kinesis stream.
```
python3 das_to_json.py <RESOURCE_ID>(cluster-xxx) | tee <AUMY_DAS_JSON>
```

## 制限事項
* 再起動などをしてセッションIDが重複している場合異なるセッションと紐づけらることがあります
