# cloud-bigdata-project

## Project structure
All scripts that were used to reach our conclusions are stored in the `/scripts` folder in the root of this repo.

To streamline our workflow, we are using environment variables to set workspace-depending variables, such as the path to the dataset. So, to succesfully run scripts, you may create an `.env` file similar to `.env.example`

It is also worth mentioning there are two Python files in the root of the repo:
- `env_wrapper.py`: gets the env variables.
- `schema.py`: used to define the dataset's schema.

## Datasets
Original dataset:
```
https://www.kaggle.com/datasets/dilwong/flightprices
```
There are available two smaller version of this dataset (1MB each) uploaded on this repo.

To obtain a smaller dataset than the original, this command can be used to take 1 out of every 30 lines, obtaining a 1GB dataset.
```
sed -n '1~30p' itineraries.csv > small_itineraries.csv
```

## PySpark on Google Cloud
There will be a single Bucket shared accross the team.

First, we have to assign the following permissions in your Bucket to the Google service account (which belongs to the cluster that will be used to run scripts) "Storage Legacy Bucket Owner" and "Storage Legacy Object Owner".

The cluster's service account email is found on the master VM's details.

![image](https://user-images.githubusercontent.com/45616341/207146875-a643c4ec-3688-421c-bc2e-fc1b686b81a4.png)

Then, the cluster will be able to access the shared Bucket.

## Running PySpark on a cluster
The following commands have to be run to ensure we have the dependencies needed and environment variables set.

```
python -m pip install python-dotenv
```
```
BUCKET=gs://pacolo2
export FILENAME=$BUCKET/itineraries.csv
export OUTPUT_DIR=$BUCKET/scripts_output/
```

Then, to run scripts, the following command can be used:
```
spark-submit --py-files $BUCKET/env_wrapper.py,$BUCKET/schema.py $BUCKET/scripts/<SCRIPT-NAME>.py
```

## Running PySpark locally
To run a script on your local machine, you have to have a `.env` file with the variables indicated by the `.env.example` file.

Then, to run scripts, you can use the following command:
```
spark-submit --py-files ../env_wrapper.py,../schema.py <SCRIPT-NAME>.py
```

There is an example script to test the correct functioning of your environment: `example.py`