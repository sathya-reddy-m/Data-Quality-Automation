# Data Quality Automation

## Build image
```
docker build -t spark_scala .
```

## Playground

Enter image
```
docker run -it --rm \
-v ${PWD}/data_quality_automation:/app \
-v ${PWD}/sample_data:/sample_data \
spark_scala bash
```

Build solution
```
sbt package
```

## Extract stats and failing rows from a single file

### Local IO
```
spark-submit \
  --class "QualityCheckerGlobalMain" \
  --master local[4] \
  target/scala-2.11/dataqualityautomation_2.11-0.0.1.jar \
  --config_file config/ZA/accounts.json \
  --storage_input_file /sample_data/za_accounts_20191125.csv \
  --storage_output_dir /sample_data/za \
  --save_failing_rows
```

**Parameters:**

* `--config_file`: a **json** file depicting constraints for a given entity
* `--storage_input_file`: Input **csv** file
* `--storage_output_dir`: Output directory
* `--save_failing_rows`: Flag that specify whether or not to save failing rows

### Blob Storage IO
```
spark-submit \
  --class "QualityCheckerGlobalMain" \
  --master local[4] \
  target/scala-2.11/dataqualityautomation_2.11-0.0.1.jar \
  --config_file /config/ZA/items.json \
  --storage_input_file za/processed/za_items_20191125.csv \
  --storage_output_dir ZA \
  --storage_account_name b2bfilemgmtsagbqa \
  --storage_container_name recommender \
  --storage_account_access_key kP+142HGNObuUz2Jp/yNLNaIRRVOepO2ClPR8/kTqK2bREUrHA4RDJZYT89DVJVdHrSL2onxOggjVLAzcXBfgg== \
  --save_failing_rows
```

**Parameters:**

* `--config_file`: a **json** file depicting constraints for a given entity
* `--storage_input_file`: Input **csv** file at blob storage
* `--storage_output_dir`: Output directory at blob storage
* `--storage_account_name`: Blob Storage account name
* `--storage_account_access_key`: Blob Storage acess key
* `--storage_container_name`: Blob Storage container name
* `--save_failing_rows`: Flag that specify whether or not to save failing rows

## Extract stats and failing rows for all files inside a blob storage

```
spark-submit \
  --class "ScanAndCheckGlobal" \
  --master local[4] \
  target/scala-2.11/dataqualityautomation_2.11-0.0.1.jar \
  --config_file /config/ZA/accounts.json \
  --storage_input_dir za/processed \
  --storage_account_name b2bfilemgmtsagbqa \
  --storage_container_name recommender \
  --storage_account_access_key kP+142HGNObuUz2Jp/yNLNaIRRVOepO2ClPR8/kTqK2bREUrHA4RDJZYT89DVJVdHrSL2onxOggjVLAzcXBfgg== \
  --file_prefix za_accounts_2019
  --save_failing_rows
```

**Parameters:**

* `--config_file`: a **json** file depicting constraints for a given entity
* `--storage_input_dir`: Blob Storage path to input **csv** files
* `--storage_account_name`: Blob Storage account name
* `--storage_account_access_key`: Blob Storage acess key
* `--storage_container_name`: Blob Storage container name
* `--save_failing_rows`: Flag that specify whether or not to save failing rows
* `--file_prefix`: File prefix for pattern matching used to list files to be processed



## Do processing on Databricks

This is **recommended** for large files (+1GB)

Install [Databricks CLI](https://docs.databricks.com/dev-tools/databricks-cli.html)

Configure the databricks access token
```
databricks configure --token

Databricks Host: https://westeurope.azuredatabricks.net/
Token: dapi71c11c55eb1be0fcd74ae3bfe9493317
```

To create another token, please read the [official doc](https://docs.databricks.com/dev-tools/api/latest/authentication.html)


### Transfer jar to DBFS
```
dbfs cp --overwrite ./data_quality_automation/target/scala-2.11/dataqualityautomation_2.11-0.0.1.jar dbfs:/FileStore/job-jars/dataqualityautomation_2.11-0.0.1.jar
```

### Create Job

#### Check for single file

```
databricks jobs create --json-file ./data_quality_automation/jobs/QualityCheckerGlobal.json
```

The above command will output the job_id for the newly created job like this:
```
{
  "job_id": 218
}
```
Use this `job_id` to submit jobs

#### Scan blob and process each file individually
```
databricks jobs create --json-file ./data_quality_automation/jobs/ScanAndCheckGlobal.json
```


### Submit job (example)

#### Check for single file

```
databricks jobs run-now \
  --job-id 218 \
  --jar-params '["--config_file","/config/ZA/items.json","--storage_input_file","za/processed/za_items_20191209.csv","--storage_output_dir","ZA","--storage_account_name","b2bfilemgmtsagbqa","--storage_container_name","recommender","--storage_account_access_key","kP+142HGNObuUz2Jp/yNLNaIRRVOepO2ClPR8/kTqK2bREUrHA4RDJZYT89DVJVdHrSL2onxOggjVLAzcXBfgg==","--save_failing_rows"]'
```

The above command will output the run_id and the corresponding job number

```
{
  "run_id": 654,
  "number_in_job": 1
}
```

#### Scan blob and process each file individually
```
databricks jobs run-now \
  --job-id 219 \
  --jar-params '["--config_file","/config/ZA/items.json","--storage_input_dir","za/processed","--storage_account_name","b2bfilemgmtsagbqa","--storage_container_name","recommender","--storage_account_access_key","kP+142HGNObuUz2Jp/yNLNaIRRVOepO2ClPR8/kTqK2bREUrHA4RDJZYT89DVJVdHrSL2onxOggjVLAzcXBfgg==","--save_failing_rows","--file_prefix","za_items_2"]'
```
