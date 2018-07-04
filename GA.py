"""Google Analytics API V4 used"""
# -*- coding: utf-8 -*-
# encoding=utf8
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
"""Import database settings from db file"""
from db import db
import time


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '/home/ubuntu/ga-api/preply-seo-concsole-e65b56dfb586.json'
VIEW_ID = '147771775'
my_date = raw_input("enter date like: 2018-06-25  ")



def fetch():
    c = db.cursor()
    c.execute("""SELECT DISTINCT url FROM search_console_data WHERE (date=%s and nusers=0) AND (version = 1 or version = 2)""", (my_date,))
    fetch = c.fetchall()
    for f_row in fetch:
        url = f_row[0]
        yield url

def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics

def get_report(analytics, url):

  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate':my_date, 'endDate':my_date}],
          'metrics': [{'expression': 'ga:newUsers'}],
          'dimensions': [{'name': 'ga:landingPagePath'}],
          'dimensionFilterClauses': [{'filters': [{"dimensionName": "ga:landingPagePath","operator": "EXACT","expressions": [url.decode('windows-1252')]}]}],
          'samplingLevel': 'LARGE'
        }]
      }
  ).execute()



def print_response(response, url):
    x = db.cursor()
    for report in response.get('reports', []):

        for row in report.get('data', {}).get('rows', []):
          dateRangeValues = row.get('metrics', [])

          for value in dateRangeValues:
              val = (value.get('values'))
              x.execute("""UPDATE search_console_data SET nusers = (%s) WHERE url = (%s) AND date = (%s)""", (val, url, my_date))
              db.commit()



def main():
    analytics = initialize_analyticsreporting()
    urls = fetch()
    for url in urls:
        response = get_report(analytics, url)
        print_response(response, url)
        time.sleep(1)
        print url + " done\n******"

if __name__ == '__main__':
  main()