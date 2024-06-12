# -*- coding: utf-8 -*-
"""Google_Analytics_UA_data_backup.ipynb
# I highly recommend that you run this code in Colab.
# Archive Universal Analytics (UA) Data Using Google Analytics API
How to run this notebook in Google Colab.
1.   Create a GCP project. In APIs and Services, enable Analytics Reporting API and Cloud Storage (if you want to export to cloud storage).
2.   Create a service account, download the key json.
3.   Click on the key in left pane of the notebook in Google Colab and save the key json text as an environmental variable called ua_sa_key.

### Load Libraries
"""

import numpy as np
import pandas as pd
from google.oauth2 import service_account
from apiclient.discovery import build
from google.colab import userdata
import json

"""### Declare Your Variables
In Google Anlaytics, get your UA view ID (in Admin ->  View -> View Settings) and add your service account to the users of the GA data.

For output metrics and dimension names refer to https://ga-dev-tools.google/dimensions-metrics-explorer/
"""

your_view_id = '128232928'
ga_keys = json.loads(userdata.get('ua_sa_key'))

start_date = '2021-01-01'
end_date = '2023-07-31'
output_metrics = [{'expression': 'ga:sessions'},
                  {'expression': 'ga:users'},
                  {"expression": "ga:avgSessionDuration"},
                  {"expression": "ga:pageviews"},
                  {"expression": "ga:uniquePageviews"}]

output_dimensions = [{"name": 'ga:yearMonth'},
                     {"name": "ga:channelGrouping"},
                     {"name": "ga:deviceCategory"},
                     {"name": "ga:country"},
                     {"name": "ga:sourceMedium"}]

"""### Define Procedures"""

def create_body(body, view_id, page_size = 10000, page_token = '0'):
  body['reportRequests'][0]['viewId'] = view_id
  body['reportRequests'][0]['pageSize'] = page_size
  body['reportRequests'][0]['pageToken'] = page_token

  return body


def format_summary(response):
    try:
        # create row index
        try:
            row_index_names = response['reports'][0]['columnHeader']['dimensions']
            row_index = [ element['dimensions'] for element in response['reports'][0]['data']['rows'] ]
            row_index_named = pd.MultiIndex.from_arrays(np.transpose(np.array(row_index)),
                                                        names = np.array(row_index_names))
        except:
            row_index_named = None

        # extract column names
        summary_column_names = [item['name'] for item in response['reports'][0]
                                ['columnHeader']['metricHeader']['metricHeaderEntries']]

        # extract table values
        summary_values = [element['metrics'][0]['values'] for element in response['reports'][0]['data']['rows']]

        # combine. I used type 'float' because default is object, and as far as I know, all values are numeric
        df = pd.DataFrame(data = np.array(summary_values),
                          index = row_index_named,
                          columns = summary_column_names).astype('float')

    except:
        df = pd.DataFrame()

    return df

def format_pivot(response):
    try:
        # extract table values
        pivot_values = [item['metrics'][0]['pivotValueRegions'][0]['values'] for item in response['reports'][0]
                        ['data']['rows']]

        # create column index
        top_header = [item['dimensionValues'] for item in response['reports'][0]
                      ['columnHeader']['metricHeader']['pivotHeaders'][0]['pivotHeaderEntries']]
        column_metrics = [item['metric']['name'] for item in response['reports'][0]
                          ['columnHeader']['metricHeader']['pivotHeaders'][0]['pivotHeaderEntries']]
        array = np.concatenate((np.array(top_header),
                                np.array(column_metrics).reshape((len(column_metrics),1))),
                               axis = 1)
        column_index = pd.MultiIndex.from_arrays(np.transpose(array))

        # create row index
        try:
            row_index_names = response['reports'][0]['columnHeader']['dimensions']
            row_index = [ element['dimensions'] for element in response['reports'][0]['data']['rows'] ]
            row_index_named = pd.MultiIndex.from_arrays(np.transpose(np.array(row_index)),
                                                        names = np.array(row_index_names))
        except:
            row_index_named = None
        # combine into a dataframe
        df = pd.DataFrame(data = np.array(pivot_values),
                          index = row_index_named,
                          columns = column_index).astype('float')
    except:
        df = pd.DataFrame()
    return df

def format_report(response):
    summary = format_summary(response)
    pivot = format_pivot(response)
    if pivot.columns.nlevels == 2:
        summary.columns = [['']*len(summary.columns), summary.columns]

    return(pd.concat([summary, pivot], axis = 1))

def run_report(body, view_id, credentials_json, page_size=10000):
    #Create service credentials
    credentials = service_account.Credentials.from_service_account_info(credentials_json,
                                scopes = ['https://www.googleapis.com/auth/analytics.readonly'])
    #Create a service object
    service = build('analyticsreporting', 'v4', credentials=credentials)


    #Get GA data
    page_token = '0'
    response = service.reports().batchGet(body=create_body(body, view_id, page_size, page_token)).execute()
    df = format_report(response)

    # If the response has nextPageToken, continue
    while ('nextPageToken' in response['reports'][0].keys()):
        page_token = response['reports'][0]['nextPageToken']
        response = service.reports().batchGet(body=create_body(body, view_id, page_size, page_token)).execute()
        df = pd.concat([df, format_report(response)])


    return(df)

"""### Construct your Request"""

monthly_request = {'reportRequests': [{'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                                       'metrics': output_metrics,
                                       'dimensions': output_dimensions,

                                       }]}

"""### Run Your Report and Validate Output"""

ga_report = run_report(monthly_request, your_view_id, ga_keys)
ga_report

"""### Export Your Output to Google Drive or Google Storage

To export .csv to Google Drive, create a folder to hold the export first. Then mount the drive - authorize your colab notebook to access the drive - and then export the dataframe to the drive. For reference, my folder is called 'Colab_outputs'.
"""

from google.colab import drive
drive.mount('/content/drive')

ga_report.to_csv('/content/drive/My Drive/Colab_outputs/ga4_2021_2023_monthly.csv', index=True)

"""Before you export the data to Google Storage, go to GCP, create a could storage bucket and give your service account access: make your service account principal with a role - Storage Objects Admin."""

from google.cloud import storage
your_bucket_name = 'ua_backup_zyabkina'
your_output_file_name = 'ga4_2021_2023_monthly.csv'

client = storage.Client.from_service_account_info(ga_keys)
bucket = client.bucket(your_bucket_name)
blob = bucket.blob(your_output_file_name)

# your output will be overwritten if it exists
blob.upload_from_string(ga_report.to_csv(), 'text/csv')