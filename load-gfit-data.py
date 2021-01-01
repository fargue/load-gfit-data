#! /usr/bin/env python3

import json
import requests
from requests.exceptions import HTTPError
import pprint
import datetime
import psycopg2
import argparse
import sys
import os

from dotenv import load_dotenv

verbose = False

def log(msg):
    print(msg)
    return

def debug(msg):
    if verbose:
        log(msg)
    return

def error(msg):
    log(msg)
    sys.exit(1)
    return

def sleepStage(key):
    d = {
        1: 'Awake (during sleep cycle)',
        2: 'Sleep',
        3: 'Out-of-bed',
        4: 'Light Sleep',
        5: 'Deep sleep',
        6: 'REM'
    }
    return d[key]

def connectToPg(cfg):
    try:
        cnx = psycopg2.connect(**cfg)
    except (Exception, psycopg2.Error) as err:
        error("Error while connecting to postgreql: {}".format(err))
    else:
        log("Connected to database")

    return cnx

# This function creates a new Access Token using the Refresh Token
# and also refreshes the ID Token(see comment below).
def refreshToken(client_id, client_secret, refresh_token):

    params = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token
    }

    authorization_url = "https://www.googleapis.com/oauth2/v4/token"

    r = requests.post(authorization_url, data=params)

    if r.ok:
            return r.json()['access_token']
    else:
            return None

def getSessionInfo(accessToken,ses,db,sid):
    url = 'https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate'
    iStmt = """
        insert into healthdata.sleep_session_data
            ( sleep_session_id, sleep_stage_id, 
              start_date, start_nanoseconds, 
              end_date, end_nanoseconds, duration_ms)
            values
            ( %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT sleep_session_data_start_date_end_date_key
                DO UPDATE SET
                    start_nanoseconds = EXCLUDED.start_nanoseconds,
                    end_nanoseconds = EXCLUDED.end_nanoseconds,
                    duration_ms = EXCLUDED.duration_ms,
                    sleep_stage_id = EXCLUDED.sleep_stage_id
            RETURNING id
    """
    try:
        
        reqData = {
            'aggregateBy': {
                'dataTypeName': 'com.google.sleep.segment'
            },
            'endTimeMillis': '{}'.format(ses['endTimeMillis']),
            'startTimeMillis': '{}'.format(ses['startTimeMillis'])
        }
        reqHeaders = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer {}'.format(accessToken)
        }
        #log(reqData)
        response = requests.post(url, headers=reqHeaders, json=reqData)
        #log(response.content)
        response.raise_for_status()
        jsonResponse = json.loads(response.text)
        #pp = pprint.PrettyPrinter(indent=2)
        #pp.pprint(jsonResponse)
        for activity in jsonResponse['bucket'][0]['dataset'][0]['point']:
            durationMs = (int(activity['endTimeNanos']) - int(activity['startTimeNanos']))/1000000
            # convert epoch to localtime
            fmt = "%Y-%m-%d %H:%M:%S"
            # local time
            t = datetime.datetime.fromtimestamp(
                float(activity['startTimeNanos'])/1000000000.)
            startDate = t.strftime(fmt)  # logs 2012-08-28 02:45:17
            t = datetime.datetime.fromtimestamp(
                float(activity['endTimeNanos'])/1000000000.)
            endDate = t.strftime(fmt)  # logs 2012-08-28 02:45:17

            debug('Activity: startNanos: {}, endNanos: {}, valueType: {}'.format(
                    activity['startTimeNanos'],
                    activity['endTimeNanos'], 
                    sleepStage(activity['value'][0]['intVal'])))
            try:
                cInsert = db.cursor()
                cInsert.execute(iStmt, (
                    sid, 
                    activity['value'][0]['intVal'],
                    startDate,
                    activity['startTimeNanos'],
                    endDate,
                    activity['endTimeNanos'],
                    durationMs
                    )
                )
            except (Exception, psycopg2.Error) as error:
                error('Failed inserting session record: {}'.format(error))
                return
            finally:
                cInsert.close()

    except HTTPError as http_err:
        error(f'HTTP error occurred: {http_err}')
    except Exception as err:
        error(f'Other error occurred: {err}')
    return


def processSession(accessToken,ses,db):
    # convert epoch to localtime
    fmt = "%Y-%m-%d %H:%M:%S"
    # local time
    t = datetime.datetime.fromtimestamp(float(ses['startTimeMillis'])/1000.)
    startDate = t.strftime(fmt)  # logs 2012-08-28 02:45:17
    t = datetime.datetime.fromtimestamp(float(ses['endTimeMillis'])/1000.)
    endDate = t.strftime(fmt)  # logs 2012-08-28 02:45:17
    durationMs = int(ses['endTimeMillis']) - int(ses['startTimeMillis'])
    if verbose:
        debug('StartDateTime: {}, EndDateTime: {}, DurationMS: {}, StartMillis: {}, EndMillis: {}'.format(
            startDate, endDate, durationMs, ses['startTimeMillis'], ses['endTimeMillis']))
    else:
        log("Processing start_date: {}, end_date: {}".format(startDate, endDate))
    # insert data into table
    sessionId = None
    try:
        cInsert = db.cursor()
        iStmt = """
            insert into healthdata.sleep_session
                ( start_date, start_milliseconds, end_date, end_milliseconds, duration_ms)
                values
                ( %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT sleep_session_start_date_end_date_key
                    DO UPDATE SET
                        start_milliseconds = EXCLUDED.start_milliseconds,
                        end_milliseconds = EXCLUDED.end_milliseconds,
                        duration_ms = EXCLUDED.duration_ms
                RETURNING id
        """
        cInsert.execute( iStmt, ( startDate, ses['startTimeMillis'], endDate, ses['endTimeMillis'], durationMs ))
        sessionId = cInsert.fetchone()[0]
        #rowCount = cInsert.rowcount
        #log('upserted {} rows'.format(rowCount))
        getSessionInfo(accessToken, ses, db, sessionId)
    except (Exception, psycopg2.Error) as error:
        error('Failed inserting session record: {}'.format(error))
        return
    finally:
        cInsert.close()
    return

def main():
    # only pulls the last 30 days and I could not get pageTokens to work
    # https://developers.google.com/fit/rest/v1/reference/users/sessions/list
    # I'm going to use startDate and endDate.  
    url = 'https://www.googleapis.com/fitness/v1/users/me/sessions'

    global verbose
    # initiate arg parser
    parser = argparse.ArgumentParser()

    # Add long and short args
    parser.add_argument('--verbose', '-v', help='verbose logging', action='store_true')
    parser.add_argument('--refreshtoken', '-r', help='user refresh token', required=True)
    parser.add_argument('--daysback', '-d', help='How many days back to look for activity (default=30)', default=30)

    args = parser.parse_args()

    if args.verbose:
        verbose = True
    else:
        verbose = False
    
    if verbose:
        debug("Running in debug mode")

    log('looking back {} days'.format(args.daysback))

    refresh_token = args.refreshtoken


    load_dotenv()  # take environment variables from .env.

    accessToken = refreshToken(
        os.getenv("OAUTH_CLIENT_ID"), os.getenv("OAUTH_CLIENT_SECRET"), refresh_token)

    pgConfig = {
        'user': 'app_user',
        'password': 'hd',
        'host': 'localhost',
        'database': 'healthdata',
        'port': 25432
    }

    # get our start and end dates from daysback value
    today = datetime.datetime.now()
    p_start_date = today - datetime.timedelta(days=args.daysback)

    log('Using python values: start_date: {}, end_date: {}'.format(p_start_date,today))

    # convert dates to RFC3339: start date at midnight: 'YYYY-MM-DDT00:00:00.00Z', end_date at 1 ms before midnight: 'YYYY-MM-DDT23:59:59.99Z'
    startDate = '%sT00:00:00.00Z' % (p_start_date.strftime('%Y-%m-%d'))
    endDate = '%sT23:59:59.99Z' % (today.strftime('%Y-%m-%d'))
    log('API query param date values: start_date: {}, end_date: {}'.format(startDate,endDate))

    pgc = connectToPg(pgConfig)

    try:
        reqHeaders = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer {}'.format(accessToken)
        }
        #log(reqHeaders)
        payload = {'activityType': 72, 'startTime': startDate, 'endTime': endDate}
        response = requests.get(url, headers=reqHeaders, params=payload)
        #log(response.content)
        response.raise_for_status()
        jsonResponse = json.loads(response.text)
        if verbose:
            pp = pprint.PrettyPrinter(indent=2)
            pp.pprint(jsonResponse)
        for ses in jsonResponse['session']:
            processSession(accessToken,ses,pgc)

        pgc.commit()
        pgc.close()
    except HTTPError as http_err:
        pgc.rollback()
        pgc.close()
        error(f'HTTP error occurred: {http_err}')
    except Exception as err:
        pgc.rollback()
        pgc.close()
        error(f'Other error occurred: {err}')

    log("Finished...")

main()
