"""Google Analytics API V4 used"""
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
"""Import database settings from db file"""
from db import db
import time
from contextlib import closing

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '/home/ubuntu/ga-api/preply-seo-concsole-e65b56dfb586.json'
VIEW_ID = '147771775'
my_date = raw_input("enter date like: 2018-06-25  ")



def fetch():
    with closing(db.cursor()) as c:
        c.execute("""SELECT DISTINCT url FROM search_console_data WHERE (date=%s and user_check=0)""", (my_date,))
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
    with closing(db.cursor()) as x:
        for report in response.get('reports', []):

            for row in report.get('data', {}).get('rows', []):
              dateRangeValues = row.get('metrics', [])

              for value in dateRangeValues:
                  val = (value.get('values'))
                  x.execute(
                      """UPDATE search_console_data SET nusers = (%s), user_check = 1 WHERE url = (%s) AND date = (%s)""",
                      (val, url, my_date))
                  x.execute("""INSERT INTO ga_urls (ganusers, gaurl, gadate) VALUES (%s, %s, %s)""", (val, url, my_date))
                  db.commit()



def main():
    try:
        analytics = initialize_analyticsreporting()
        urls = fetch()
        for index, url in enumerate(urls):
            response = get_report(analytics, url)
            print_response(response, url)
            time.sleep(0.5)
            print index, my_date, url + " done\n******"
    except Exception as e:
        print e.message, e.args
    except HttpError, error:
        if error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded',
                                 'internalServerError', 'backendError']:
            print "GA API error"

if __name__ == '__main__':
  main()