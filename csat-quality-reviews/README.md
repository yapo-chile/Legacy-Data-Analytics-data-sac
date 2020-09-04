# csat-quality-reviews pipeline 

# csat-quality-reviews

## Description

This pipeline makes available data associated with customer service
performed by the SAC and Quality team. From this data you can obtain information
associated with number of customer moderations for different dimensions adding the information
to a level where it balances the degree of aggregation with the specificity we need.
This data is obtained through the interaction with various tables in our datawarehouse
and and is recorded into datawarehouse after being transformed into this pipeline.

Finally, it is important to mention that regarding the execution and re-execution strategy, it is necessary in this case to pass variables "from" "to" (dates vars).

## Pipeline Implementation Details

|   Field           | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| Input Source      | DWH: stg.ads_reviews_time                                                   |
|                   |      stg.review_params                                                      |
|                   |      ods.ad                                                                 |  
|                   |      ods.seller_pro_details                                                 |
|                   |      ods.category                                                           |
| Output Source     | DWH: dm_content_sac.pro_reviews                                             |
|                   |      dm_content_sac.agg_reviews                                             |
| Schedule          | At 09:00 AM                                                                 |
| Rundeck Access    | data jobs: GLOBAL-METRIC: Peak - Content-SAC                                |
| Associated Report | Content&Sac SQUAD:                                                          |
|                   |    https://tableau.mpi-internal.com/#/site/sch-cl-yapo/workbooks/12268/views|
|                   | SAC KPIs:                                                                   |
|                   |    https://tableau.mpi-internal.com/#/site/sch-cl-yapo/workbooks/7559/views |
|                   |                                                                             |


### Build
```
make docker-build
```

### Run micro services
```
sudo docker run --rm -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                        -e APP_DW_SECRET=/app/db-secret \
                        containers.mpi-internal.com/yapo/csat-quality-reviews:latest
```

### Run micro services with parameters

```
sudo docker run --rm -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                        -e APP_DW_SECRET=/app/db-secret \
                        containers.mpi-internal.com/yapo/csat-quality-reviews:latest
                        -date_from=YYYY-MM-DD \
                        -date_to=YYYY-MM-DD
```

### Adding Rundeck token to Travis

If we need to import a job into Rundeck, we can use the Rundeck API
sending an HTTTP POST request with the access token of an account.
To add said token to Travis (allowing travis to send the request),
first, we enter the user profile:
```
<rundeck domain>:4440/user/profile
```
And copy or create a new user token.

Then enter the project settings page in Travis
```
htttp://<travis server>/<registry>/<project>/settings
```
And add the environment variable RUNDECK_TOKEN, with value equal
to the copied token
