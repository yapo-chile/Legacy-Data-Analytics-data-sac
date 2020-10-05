# net-promoter-score pipeline 

# net-promoter-score

## Description

This pipeline makes available data associated with customer service performed by the SAC team. From this data you can obtain information associated with NPS indicator which is an indicator that proposes to measure the loyalty of a company's clients based on the recommendations. The Net Promoter Score is based on a single question: "How likely are you to recommend the product or service to a family member or friend?". To do this, they are asked to rate on a scale from 0 to 10, where 0 is "Very unlikely" and 10 is "I would definitely recommend it." According to the results, clients are classified into promoters, liabilities and detractors. This data is obtained through the interaction with endpoint available on the Surveypal Api platforms and is recorded in our datawarehouse after being transformed into this pipeline.

Finally, it is important to mention that regarding the execution and re-execution strategy, it is necessary in this case to pass variable "date_from" (date var).

## Pipeline Implementation Details

|   Field           | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| Input Source      | Surveypal API endpoints                                                     |
| Output Source     | DWH: dm_content_sac.nps_answers                                             |
|                   |      dm_content_sac.temp_nps_answers                                        |
| Schedule          | At 09:00 AM                                                                 |
| Rundeck Access    | data jobs: SAC: Net-promoter-score                                          |
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
                        -v /home/bnbiuser/secrets/secrets_surveypal:/app/surveypal-api-secret \
                        -e APP_DW_SECRET=/app/db-secret \
                        -e APP_SURVEYPAL_API_SECRET=/app/surveypal-api-secret \
                        containers.mpi-internal.com/yapo/net-promoter-score:latest
```

### Run micro services with parameters

```
sudo docker run --rm -v /home/bnbiuser/secrets/dw_db:/app/db-secret \
                        -v /home/bnbiuser/secrets/secrets_surveypal:/app/surveypal-api-secret \
                        -e APP_DW_SECRET=/app/db-secret \
                        -e APP_SURVEYPAL_API_SECRET=/app/surveypal-api-secret \
                        containers.mpi-internal.com/yapo/net-promoter-score:latest \
                        -date_from=YYYY-MM-DD
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
