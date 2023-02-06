# Question 1. Load January 2020 data 
Using the etl_web_to_gcs.py flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

## Relevant output from the command prompt

19:57:55.110 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
19:57:55.112 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
19:57:56.626 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()

## ANS: rows: 447770

# Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows.
Using the flow in etl_web_to_gcs.py, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

## ANS: 0 5 1 * * 

Question 3. Loading data to BigQuery

Using etl_gcs_to_bq.py as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

    * 14,851,920
    * 12,282,990
    * 27,235,753
    * 11,338,483

*Ways to get to the ans: Run el_web_to_gcs.py and el_gcs_to_bq.py. Then run query: "SELECT COUNT(*) FROM `dezoomcamp_ny_taxi.yellow`;" in BigQuery.*

## ANS: 14851920

# Question 4. Github Storage Block

Using the web_to_gcs script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image.

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

    88,019
    192,297
    88,605
    190,225

## Steps:
* Upload deployment repository to github (https://github.com/Mahdi-Moosa/DE-Zoomcamp_Week-2-HW_Prefect_GitHub-deployment).
* run command: prefect deployment build etl_web_to_gcs.py:etl_parent_flow -sb github/github-hw-2-de-zoomcamp -n github_deployment_parent_flow -a
*Note: The deployment build command needs local python file (apparenly to perfrom an intial check to generat the deployment yaml file).*
* run command: prefect deployment apply etl_parent_flow-deployment.yaml
* run command: prefect agent start --work-queue "default"

**Run with specified parameters, i.e., Nov 2020 (Green Taxi).**
*Output*
20:13:35.961 | INFO    | Task run 'clean-2c6af9f6-0' - columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
20:13:35.962 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 88605

## ANS: rows: 88605

# Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur.

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up.

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the **Green** taxi data for **April 2019**. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook.

Join my temporary Slack workspace with this link. 400 people can use this link and it expires in 90 days.

In the Prefect Cloud UI create an Automation or in the Prefect Orion UI create a Notification to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create.

How many rows were processed by the script?

    125,268
    377,922
    728,390
    514,392

*Solution*
Commands: 
*Step 1*: Create prefect cloud account.
*Step 2:* Run: prefect cloud login -k MY_PREFECT_API_KEY
*Step 3:* GCS bucket was not present in the Blocks. Added by running command: prefect block register -m prefect_gcp
*Step 4:* Add gc credentials block, add GCS buckets block.
*Step 5:* Run: prefect deployment build flows/etl_web_to_gcs.py:etl_parent_flow -n cloud_flow_deployment -sb github/github-hw-2-de-zoomcamp --apply
*Step 6:* Run: prefect agent start --work-queue "default"
*Step 7:* Trigger run from prefect cloud UI.

Outpur: rows: 514392

## ANS: rows: 514392

# Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

    5
    6
    8
    10

Value
********

## ANS: 8
