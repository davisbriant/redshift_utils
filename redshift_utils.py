import boto3
import time, os
from datetime import datetime
import sys
import utils, log_utils
from ip_utils import ip_utils
import psycopg2
import json
import gzip
import logging
from joblib import Parallel, delayed

logger = logging.getLogger(__name__)

def processTstamps(config, cluster, db, currentLoadTstamps):
    try: 
        clientSql = "dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(db, config['redshift']['user'], config['redshift']['clients']['host'], config['redshift']['password'], config['redshift'][cluster]['port'])
        clientConn = psycopg2.connect(clientSql)
    except:
        logger.error("I am unable to connect to the client database: {}".format(db))   
    try:
        rmulusSql = "dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(config['redshift']['rmulus']['dbname'], config['redshift']['user'], config['redshift']['rmulus']['host'], config['redshift']['password'], config['redshift'][cluster]['port'])
        rmulusConn = psycopg2.connect(rmulusSql)
    except:
        logger.error("I am unable to connect to the rmulus database: {}".format(rmulusDb))

    rmulusCur = rmulusConn.cursor()
    clientCur = clientConn.cursor()
    for tstamp in currentLoadTstamps:
        kwargs = {}
        currentLoadTstamp = tstamp
        dateRange = "select distinct date_trunc('day', event_timestamp) FROM rmulus WHERE _pclientId = '{}' AND ingested_timestamp = '{}' ORDER BY date_trunc('day', event_timestamp) ASC".format(db, currentLoadTstamp)
        rmulusCur.execute(dateRange)
        dateRange = []
        for dates in rmulusCur.fetchall():
            dateRange.append(dates[0])
        logger.info("dates to unload for ingested timestamp {} are {}".format(currentLoadTstamp, dateRange))
        #now get the hours per date per ingested timestamp to be processed
        for date in dateRange:
            day = date.strftime("%Y-%m-%d")
            getHours = "select distinct date_part('hour', event_timestamp) FROM rmulus WHERE _pclientId = '{}' AND ingested_timestamp = '{}' AND date_trunc('day', event_timestamp) = '{}' ORDER BY date_part('hour', event_timestamp) ASC".format(db, currentLoadTstamp, day)
            rmulusCur.execute(getHours)
            getHours = rmulusCur.fetchall()
            hours = []
            for hour in getHours:
                hours.append(int(hour[0]))
            logger.info("Hours to process for {} are {}".format(day, hours))
            #set the start and end hours to loop through in sql per date per ingested timestamp to be processed
            startHour = hours[0]
            finalHour = hours[-1]
            #define the start and end timestamps for each hour to be written in the filename unloaded to s3
            startingHours = ['00:00:00','01:00:00','02:00:00','03:00:00','04:00:00','05:00:00','06:00:00','07:00:00','08:00:00','09:00:00','10:00:00','11:00:00','12:00:00','13:00:00','14:00:00','15:00:00','16:00:00','17:00:00','18:00:00','19:00:00','20:00:00','21:00:00','22:00:00','23:00:00']
            endingHours = ['00:59:59','01:59:59','02:59:59','03:59:59','04:59:59','05:59:59','06:59:59','07:59:59','08:59:59','09:59:59','10:59:59','11:59:59','12:59:59','13:59:59','14:59:59','15:59:59','16:59:59','17:59:59','18:59:59','19:59:59','20:59:59','21:59:59','22:59:59','23:59:59'] 
            #finally start executing final sql queries broken down by ingested timestamp, date, hour and unload results to s3 using corresponding naming convention variables
            i = 0
            for hour in hours:
                d = []
                startingHour = startingHours[hour]
                endingHour = endingHours[hour]
                d.extend((config, cluster, db, currentLoadTstamp, startingHour, endingHour, day, hour))
                key = '{}_{}'.format(day, hour)
                kwargs[key] = d
                i = i + 1
        try:
            Parallel(n_jobs=-1)(delayed(processTstampDatesHours)(key, val) for key, val in kwargs.items())
        except Exception as e:
            logger.error(e)
        #once all rmulus logs per ingested timestamp per day per hour have been unloaded and their corresponding ip lookup files unloaded and copied
        region = config['aws']['region']
        #copy rmulus logs for ingested timestamp to redshift
        copy = "copy rmulus(event_timestamp, client_ip, x_forwarded_for, ip_chain_json, referrer, user_agent, device_family, ua_family_and_version, os_family_and_version, rmulus_id, rmulus_id_timestamp, _pclientid, _peventname, _pdatasource, json_payload, ingested_timestamp) from 's3://{}/clients/{}/{}/staged/{}_' credentials 'aws_iam_role={}' region '{}' IGNOREBLANKLINES TIMEFORMAT AS 'auto' DELIMITER AS '\t' EMPTYASNULL GZIP DATEFORMAT AS 'auto' MAXERROR AS 200".format(config['s3']['bucket'], db, config['s3']['clients_base_log_folder'], currentLoadTstamp, config['redshift']['iamRole'], region)
        try:
            clientCur.execute(copy)
            clientConn.commit()
            logger.info("rmulus logs COPY complete for ingested_timestamp '{}'".format(currentLoadTstamp))
        except Exception as e:
            logger.error(e)
        #copy ip lookups files to redshift
        copyIpLookups =  "copy lookups_pntheon_ip(_pevid, company_name, continent_code, continent_name, country_isocode, country_name, state_isocode, state_name, city, postalcode, latitude, longitude, timezone) from 's3://{}/clients/{}/{}/staged/{}_' credentials 'aws_iam_role={}' region '{}' IGNOREBLANKLINES TIMEFORMAT AS 'auto' DELIMITER AS '\t' EMPTYASNULL GZIP DATEFORMAT AS 'auto' MAXERROR AS 200".format(config['s3']['bucket'], db, config['s3']['clients_ip_lookup_folder'], currentLoadTstamp, config['redshift']['iamRole'], region)
        try: 
            clientCur.execute(copyIpLookups)
            clientConn.commit()
            logger.info("ip lookups COPY complete for '{}'".format(currentLoadTstamp))
        except Exception as e:
            logger.error(e)
        getRows = "SELECT COUNT(event_timestamp) FROM rmulus WHERE _pclientid = '{}' AND ingested_timestamp = '{}'".format(db, currentLoadTstamp)
        getUniqueEvents = "SELECT COUNT(DISTINCT(json_extract_path_text(json_payload, '_pevId'))) FROM rmulus WHERE _pclientid = '{}' AND ingested_timestamp = '{}'".format(db, currentLoadTstamp)
        try:
            rmulusCur.execute(getRows)
            getRmulusRows = rmulusCur.fetchone()[0]
            logger.info("Total rows in rmulus table in rmulus cluster for client {} with ingested_timestamp {} = {}".format(db, currentLoadTstamp, getRmulusRows))
        except Exception as e:
            logger.error(e)
        try:
            rmulusCur.execute(getUniqueEvents)
            getRmulusRows = rmulusCur.fetchone()[0]
            logger.info("Unique event ids in rmulus table in rmulus cluster for client {} with ingested_timestamp {} = {}".format(db, currentLoadTstamp, getRmulusRows))
        except Exception as e:
            logger.error(e)
        try:
            clientCur.execute(getRows)
            getClientRows = clientCur.fetchone()[0]
            logger.info("Total rows in rmulus table in clients cluster for client {} with ingested_timestamp {} = {}".format(db, currentLoadTstamp, getClientRows))
        except Exception as e:
            logger.error(e)
        try:
            clientCur.execute(getUniqueEvents)
            getClientRows = clientCur.fetchone()[0]
            logger.info("Unique event ids in rmulus table in clients cluster for client {} with ingested_timestamp {} = {}".format(db, currentLoadTstamp, getClientRows))
        except Exception as e:
            logger.error(e)
        getIpLookupsRows = "SELECT COUNT(b._pevid) FROM rmulus a INNER JOIN lookups_pntheon_ip b ON json_extract_path_text(a.json_payload, '_pevId') = b._pevid WHERE ingested_timestamp = '{}'".format(currentLoadTstamp)
        getIpLookupsUniqueEvents = "SELECT COUNT(DISTINCT(b._pevid)) FROM rmulus a INNER JOIN lookups_pntheon_ip b ON json_extract_path_text(a.json_payload, '_pevId') = b._pevid WHERE ingested_timestamp = '{}'".format(currentLoadTstamp)
        try: 
            clientCur.execute(getIpLookupsRows)
            getIpLookups = clientCur.fetchone()[0]
            logger.info("Total rows in lookups_pntheon_ip table in clients cluster for client {} with ingested_timestamp {} = {}".format(db, currentLoadTstamp, getIpLookups))
        except Exception as e:
            logger.error(e)
        try: 
            clientCur.execute(getIpLookupsUniqueEvents)
            getIpLookups = clientCur.fetchone()[0]
            logger.info("Unique event ids lookups_pntheon_ip table in clients cluster for client {} with ingested_timestamp {} = {}".format(db, currentLoadTstamp, getIpLookups))
        except Exception as e:
            logger.error(e)
    #close connections to rmulus/clients cluster dbs
    rmulusConn.close()
    clientConn.close()
        
def processTstampDatesHours(key, kwargs):
    config = kwargs[0]
    cluster = kwargs[1]
    db = kwargs[2]
    currentLoadTstamp = kwargs[3]
    startingHour = kwargs[4]
    endingHour = kwargs[5]
    day = kwargs[6]
    hour = kwargs[7]

    try: 
        clientSql = "dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(db, config['redshift']['user'], config['redshift']['clients']['host'], config['redshift']['password'], config['redshift'][cluster]['port'])
        clientConn = psycopg2.connect(clientSql)
    except:
        logger.error("I am unable to connect to the client database: {}".format(db))   
    try:
        rmulusSql = "dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(config['redshift']['rmulus']['dbname'], config['redshift']['user'], config['redshift']['rmulus']['host'], config['redshift']['password'], config['redshift'][cluster]['port'])
        rmulusConn = psycopg2.connect(rmulusSql)
    except:
        logger.error("I am unable to connect to the rmulus database: {}".format(rmulusDb))

    rmulusCur = rmulusConn.cursor()
    clientCur = clientConn.cursor()

    logger.info("data processing for date {} start time {} end time {}".format(day, startingHour, endingHour))
    unload = "unload ('select event_timestamp, client_ip, x_forwarded_for, ip_chain_json, referrer, user_agent, device_family, ua_family_and_version, os_family_and_version, rmulus_id, rmulus_id_timestamp, _pclientid, _peventname, _pdatasource, json_payload, ingested_timestamp from rmulus WHERE _pclientId = \\'{}\\' AND ingested_timestamp = \\'{}\\' AND date_trunc(\\'day\\', event_timestamp) = \\'{}\\' AND date_part(\\'hour\\', event_timestamp) = \\'{}\\'') to 's3://{}/clients/{}/{}/staged/{}_{}_{}_{}_' credentials 'aws_iam_role={}' gzip delimiter '\t'".format(db, currentLoadTstamp, day, hour, config['s3']['bucket'], db, config['s3']['clients_base_log_folder'], currentLoadTstamp, day, startingHour, endingHour, config['redshift']['iamRole'])
    rmulusCur.execute(unload)
    rmulusConn.commit()
    logger.info('data unloaded to s3 for {} {} {} {}'.format(currentLoadTstamp, day, startingHour, endingHour))
    #now that rmulus data is unloaded the s3, get the event Id and ip_chain_json fields so that the corresponding ip lookups matchup table can be unloaded to s3   
    getIps = """SELECT json_extract_path_text(json_payload, '_pevId') AS "_pevId", ip_chain_json FROM rmulus WHERE _pclientId = '{}' AND ingested_timestamp = '{}' AND date_trunc('day', event_timestamp) = '{}' AND date_part('hour', event_timestamp) = '{}' AND json_extract_path_text(json_payload, '_pevId') IS NOT NULL AND json_extract_path_text(json_payload, '_pevId') != '' AND ip_chain_json IS NOT NULL AND ip_chain_json != '' GROUP BY json_extract_path_text(json_payload, '_pevId'), ip_chain_json""".format(db, currentLoadTstamp, day, hour)
    #make sure there is at least one result with an event Id returned
    rmulusCur.execute(getIps)
    result = rmulusCur.fetchone()
    if result is not None:
        rmulusCur.execute(getIps)
        results = rmulusCur.fetchall();
        # print(results)
        resultCount = len(results)
        #define filename of ip lookups match table to be uplaoded to s3
        fname = "{}_{}_{}_{}".format(currentLoadTstamp, day, startingHour, endingHour)
        logger.info("making ip lookup table {} from {} results".format(fname, resultCount))
        #go ahead and create the ip lookups table and unload to s3
        try:
            ip_utils.doIpLookups(results, db, fname)
            logger.info("IP lookup table {} uploaded to s3".format(fname))
        except Exception as e:
            logger.error(e)
    #close connections to rmulus/clients cluster dbs
    rmulusConn.close()
    clientConn.close()
                                                    
class RedshiftUtils():

    def __init__(self, config, cluster, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.config = config
        self.region_name = config['aws']['region']
        self.aws_access_key_id = config['aws']['keyId']
        self.aws_secret_access_key = config['aws']['secretAccessKey']
        self.redshift = boto3.client('redshift', region_name=self.region_name, aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)
        self.config = utils.load_config()
        self.log_utils = log_utils.LogUtils(self.config)
        self.clusterConfig = config['redshift'][cluster]
        self.cluster = cluster
        self.clusterId = config['redshift'][cluster]['clusterId']
        self.numBackups = config['redshift'][cluster]['numBackups']
        restoreFromSnapDefaults = config['redshift']['restoreFromSnap']
        restoreFromSnapCluster = config['redshift'][cluster].get('restoreFromSnap', {})
        self.restoreFromSnapConfig = {k: v for d in [restoreFromSnapDefaults, restoreFromSnapCluster] for k, v in d.items()}
        self.rmulusHost = config['redshift']['rmulus']['host']
        self.clientHost = config['redshift']['clients']['host']
        self.user = config['redshift']['user']
        self.password = config['redshift']['password']
        self.rmulusDb = config['redshift']['rmulus']['dbname']
        self.rmulusTableName = config['redshift']['rmulusTableName']
        self.clientsDb = config['redshift']['clients']['dbname']
        self.port = config['redshift'][cluster]['port']
        self.bucket = config['s3']['bucket']
        self.rmulusBaseLogFolder = config['s3']['rmulus_base_log_folder']
        self.clientsBaseLogFolder = config['s3']['clients_base_log_folder']
        self.iamRole = config['redshift']['iamRole']
        self.region = config['redshift']['region']

    def getCluster(self):
        try:
            response = self.redshift.describe_clusters(
            ClusterIdentifier=self.clusterId,
            )
            #self.logger.info(response)
            return response
        except Exception as e: 
            self.logger.error("{} does not exist".format(self.clusterId))
            status = "unavailable"
            return status

    def getSnapshots(self):
        response = self.redshift.describe_cluster_snapshots(
            ClusterIdentifier=self.clusterId
        )
        snapshots = response['Snapshots']
        clusterSnaps = []
        returnSnaps = {}
        for snaps in snapshots:
            snap = {}
            clusterId = snaps['ClusterIdentifier']
            snapId = snaps['SnapshotIdentifier']
            snapCreatedTime = snaps['SnapshotCreateTime']
            status = snaps['Status']
            check = "{}-".format(self.clusterId)
            if (clusterId == self.clusterId) and (status == 'available') and (check in snapId):
                snap['clusterId'] = clusterId
                snap['snapId'] = snapId
                snap['createdTime'] = snapCreatedTime
                snap['createdTimestamp'] = int(str.split(snapId,'-')[1])
                clusterSnaps.append(snap)
        timestamps = [x['createdTimestamp'] for x in clusterSnaps]
        timestamps = sorted(timestamps, key=int, reverse=True)
        timestamps = {i:j for i,j in enumerate(timestamps)}
        maxTime = timestamps[0]
        finalSnap = []
        deleteSnaps = []
        deleteSnapsTimestamps = []
        for k, v in timestamps.items():
            if k >= int(self.numBackups):
                deleteSnapsTimestamps.append(v)
        for snap in clusterSnaps:
            if snap['createdTimestamp'] in deleteSnapsTimestamps:
                deleteSnaps.append(snap)
            elif snap['createdTimestamp'] == maxTime:
                finalSnap.append(snap)
        returnSnaps['restoreSnap'] = finalSnap
        returnSnaps['deleteSnaps'] = deleteSnaps
        return returnSnaps

    def createSnapshot(self):
        timestamp = int(time.time())
        snapId = "{}-{}".format(self.clusterId, timestamp)
        self.logger.info("final snapId: {}".format(snapId))
        response = self.redshift.create_cluster_snapshot(
            SnapshotIdentifier=snapId,
            ClusterIdentifier=self.clusterId,
            Tags=[
                {
                    'Key': 'generatedBy',
                    'Value': 'pipeline'
                },
            ]
        )
        snapStatus = self.getSnapStatus(snapId)
        while snapStatus != 'available':
            time.sleep(30)
            snapStatus = self.getSnapStatus(snapId) 
            #self.logger.info("backup status is '{}'".format(snapStatus))
        return response

    def getSnapStatus(self,snapId):
        response = self.redshift.describe_cluster_snapshots(
            SnapshotIdentifier=snapId
        )
        #self.logger.info(response)
        snapshot = response['Snapshots']
        snapStatus = snapshot[0]['Status']
        self.logger.info("snapId {} status is '{}'".format(snapId, snapStatus))
        return snapStatus
    
    def restoreFromSnap(self, snapId):
        #self.logger.info(self.clusterId)
        self.logger.info("restoring cluster '{}' from snapshot: '{}'".format(self.clusterId, snapId))
        restoreFromSnapConfigArgs = 'self.redshift.restore_from_cluster_snapshot('
        for key in self.restoreFromSnapConfig:
            val = self.restoreFromSnapConfig[key]
            restoreFromSnapConfigArgs += "{}={},".format(key,val)
        restoreFromSnapConfigArgs += "SnapshotClusterIdentifier='{}',".format(self.clusterId)
        restoreFromSnapConfigArgs += "ClusterIdentifier='{}',".format(self.clusterId)
        restoreFromSnapConfigArgs += "Port={},".format(self.port)
        restoreFromSnapConfigArgs += "AvailabilityZone='{}',".format(self.region)
        restoreFromSnapConfigArgs += "IamRoles=['{}',],".format(self.iamRole)
        restoreFromSnapConfigArgs += "SnapshotIdentifier='{}')".format(snapId)
        #self.logger.info(restoreFromSnapConfigArgs)
        response = eval(restoreFromSnapConfigArgs)
        #self.logger.info(response)
        return response
    
    def spinUpRedshiftCluster(self):
        clusterStatus = self.getCluster()
        self.logger.info(clusterStatus)
        if clusterStatus != 'unavailable':
            self.logger.info("{} cluster is {}".format(self.clusterId, clusterStatus))
        else:
            snapshot = self.getSnapshots()
            restoreSnap = snapshot['restoreSnap']
            #cluster = restoreSnap[0]['clusterId']
            snapId = restoreSnap[0]['snapId']
            #restore cluster from most recent pipeline snapshot
            self.restoreFromSnap(snapId)
            #get status of new cluster
            self.getClusterRestoreStatus()

    def getClusterRestoreStatus(self):
        status = self.getCluster()
        status = status['Clusters']
        for stat in status:
            if stat['ClusterIdentifier'] == self.clusterId:
                status = stat['ClusterStatus']
                self.logger.info(status)
                restoreStatus = stat['RestoreStatus']
                #self.logger.info(restoreStatus)
                restStat = restoreStatus['Status']
                self.logger.info(restStat)
                #status = 'available'
                #restStat = 'incomplete'
                if (status == 'available' and restStat == 'completed'):
                    self.logger.info('restore completed')
                    return status           
                elif (status == 'deleting'):
                    self.logger.info('cluster deleting')
                    time.sleep(30)
                    self.spinUpRedshiftCluster()
                else:
                    self.logger.info('cluster restore not yet completed')
                    time.sleep(30)
                    self.getClusterRestoreStatus()

    def spinDownRedshiftCluster(self):
        self.logger.info('cluster going down')
        self.deleteCluster()
        deleteStatus = self.getClusterDeleteStatus()
        while deleteStatus != 'unavailable':
            time.sleep(30)
            deleteStatus = self.getClusterDeleteStatus()
        self.logger.info('cluster {} deleted'.format(self.clusterId))
        #delete old snapshots
        snapshots = self.getSnapshots()
        oldSnaps = snapshots['deleteSnaps']
        #self.logger.info(oldSnaps)
        for snap in oldSnaps:
            snapId = snap['snapId']
            self.deleteSnapshot(snapId)
            #self.logger.info('old snap {} deleted'.format(snapId))

    def deleteSnapshot(self, snapId):
        response = self.redshift.delete_cluster_snapshot(
            SnapshotIdentifier=snapId,
            SnapshotClusterIdentifier=self.clusterId
        )
        self.logger.info("snapshot {} deleted".format(snapId))

    def deleteCluster(self):
        timestamp = int(time.time())
        snapId = "{}-{}".format(self.clusterId, timestamp)
        self.logger.info("final snapId: {}".format(snapId))
        response = self.redshift.delete_cluster(
            ClusterIdentifier=self.clusterId,
            SkipFinalClusterSnapshot=False,
            FinalClusterSnapshotIdentifier=snapId
        )
        return response

    def getClusterDeleteStatus(self):
        status = self.getCluster()
        if status != 'unavailable':
            status = status['Clusters']
            #self.logger.info(status)
            for stat in status:
                if stat['ClusterIdentifier'] == self.clusterId:
                    status = stat['ClusterStatus']
                    self.logger.info(status)
                    return status
        else:
            self.logger.info(status)
            return status

    def copyRmulusLogs(self):
        conn = psycopg2.connect(host=self.rmulusHost,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    dbname=self.rmulusDb)

        source_bucket = "s3://{}/{}/enriched/".format(self.bucket,self.rmulusBaseLogFolder)
        iamRole = "aws_iam_role={}".format(self.iamRole)
        region = self.region[:-1]
        copy = """copy rmulus(event_timestamp, client_ip, referrer, user_agent, device_family, ua_family_and_version, os_family_and_version, rmulus_id, rmulus_id_timestamp, _pclientid, _peventname, _pdatasource, json_payload, original_file_id, x_forwarded_for, ip_chain_json) from '{}' credentials '{}' region '{}' IGNOREBLANKLINES TIMEFORMAT AS 'auto' DELIMITER AS '\t' EMPTYASNULL GZIP DATEFORMAT AS 'auto' MAXERROR AS 200""".format(source_bucket, iamRole, region)
        #self.logger.info(copy)
        #exit()
        cur = conn.cursor()
        cur.execute(copy)
        conn.commit()
        conn.close()
        self.logger.info("Enriched Logs copied to Redshift")

    def unloadRmulusLogs(self):

        #This function will scan databases in the clients cluster for the presence of a table 'rmulus'
        #If the rmulus table exists in the clients database then the function will first determine the numbers of 'ingested timestamps' behind the clients cluster db table is from the main rmulus cluster db table
        #From there the function will breakdown the unique dates to process per incremental ingested_timestamp
        #Finally, the function will breakdown the dates per ingested timestamp by hour, write the results to s3, and then copy them to the clients cluster db rmulus table
        #In addition to writing the hourly rmulus logs per ingested timestamp to s3, an additional class ip_utils.py is invoked to created a mirror lookup table per hour including asn name and geo identifiers per unique rmulus event.
        #This ip lookup file is both written to s3 and copied to clients cluster db table 'lookups_pntheon_ip'
        #Note there is no check to see if 'lookups_pntheon_ip' table exists, therefore in new client onboarding must create both rmulus and lookups_pntheon_ip in the same SQL statement

        def get_databases(conn):
            dbs = []
            try:
                cur = conn.cursor()
                cur.execute("select datname FROM pg_database where datistemplate = false ORDER BY datname ASC")
                for db in cur.fetchall():
                    dbs.append(db[0])
                    #self.logger.info(db)
            except psycopg2.Error as e:
                self.logger.error(e)
            self.logger.info("unloading logs for clients: {}".format(dbs))
            return dbs
        def table_exists(conn, table):
            exists = False
            try:
                cur = conn.cursor()
                sql = "select exists(select relname from pg_class where relname='{}')".format(table)
                cur.execute(sql)
                exists = cur.fetchone()[0]
                self.logger.info(exists)
                cur.close()
            except psycopg2.Error as e:
                self.logger.error(e)
            return exists
        def get_table_col_names(conn, table):
            col_names = []
            try:
                cur = conn.cursor()
                sql = "select * from {} LIMIT 0".format(table)
                cur.execute(sql)
                for desc in cur.description:
                    col_names.append(desc[0])    
                cur.close()
                return col_names
            except psycopg2.Error as e:
                self.logger.error(e)
                return col_names
        try: 
            #establish connection to clients cluster default db 'pntheon'
            clientSql = "dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(self.clientsDb, self.user, self.clientHost, self.password, self.port)
            clientConn = psycopg2.connect(clientSql)
        except:
            self.logger.error("I am unable to connect to the database")
        #get list of databases in the clients cluster
        databases = get_databases(clientConn)
        #connect to each database in the clients cluster one at a time  
        for db in databases:
            try: 
                self.logger.info(db)
                clientSql = "dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(db, self.user, self.clientHost, self.password, self.port)
                clientConn = psycopg2.connect(clientSql)
            except:
                self.logger.error("I am unable to connect to the client database: {}".format(db))   
            #establish connection to main rmulus cluster default database rmulus
            try:
                rmulusSql = "dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(self.rmulusDb, self.user, self.rmulusHost, self.password, self.port)
                rmulusConn = psycopg2.connect(rmulusSql)
            except:
                self.logger.error("I am unable to connect to the rmulus database: {}".format(self.rmulusDb))
            #determine if table 'rmulus' exists in the database    
            try:
                if (table_exists(clientConn, 'rmulus') == True):
                    self.logger.info("rmulus table exists in db '{}'".format(db))
                    #check the most recent ingested timestamp for the clients db rmulus table
                    previousLoad = "select max(ingested_timestamp) FROM {}".format(self.rmulusTableName)
                    clientCur = clientConn.cursor()
                    clientCur.execute(previousLoad)
                    for tstamp in clientCur.fetchone():
                        previousLoadTstamp = tstamp
                    self.logger.info("last client COPY: {}".format(previousLoadTstamp))
                    #if there is no most recent ingested timestamp, then this is the first ETL run for the clients db, therefore get all data from main rmulus cluster table for clients db
                    if not previousLoadTstamp:
                        currentLoad = "select distinct(ingested_timestamp) from rmulus WHERE _pclientId = '{}' ORDER BY ingested_timestamp ASC".format(db)
                    #otherwise, get only the rmulus data for the clients db that occurred since the previous ingested timestamp
                    else:
                        currentLoad = "select distinct(ingested_timestamp) from rmulus WHERE _pclientId = '{}' AND ingested_timestamp > '{}' ORDER BY ingested_timestamp ASC".format(db, previousLoadTstamp)
                    rmulusCur = rmulusConn.cursor()
                    rmulusCur.execute(currentLoad)
                    #prepare list of ingested timestamps in the current load
                    currentLoadTstamps = []
                    for tstamp in rmulusCur.fetchall():
                        currentLoadTstamps.append(tstamp[0])
                    self.logger.info("current rmulus timestamps to UNLOAD: {}".format(currentLoadTstamps))
                    #check if empty table exists in both main rmulus and clients clusters
                    if not previousLoadTstamp and not currentLoadTstamps:
                        self.logger.info("no new data to unload")
                    #otherwise, check if this is either the first load for a clients db or an incremental load for a clients db
                    elif ((not previousLoadTstamp and currentLoadTstamps) or (previousLoadTstamp and currentLoadTstamps)):
                        #if this is the first load for a clients db...
                        if not previousLoadTstamp and currentLoadTstamps:
                            self.logger.info("do first unload")
                        #or if this is an incremental load for a clients db...
                        elif previousLoadTstamp and currentLoadTstamps:
                            self.logger.info("do incremental unload")
                        #in either case, get the dates for each ingested timestamp to be processed 
                        processTstamps(self.config, self.cluster, db, currentLoadTstamps)
                        #mark unload/copy process complete for ETL run
                        self.logger.info("data UNLOAD/COPY complete for '{}' with ingested timestamps '{}'".format(db, currentLoadTstamps))
                        #rotate client logs from staged to archived for ETL run
                        self.log_utils.rotateClientLogs(db, "staged", "archived")
                        self.logger.info("client rmulus logs for '{}' with ingested_timestamps '{}' rotated from /staged/ to /archived/".format(db, currentLoadTstamps))
                        #rotate client ip lookups logs from staged to archived for ETL run 
                        self.log_utils.rotateClientIpLookups(db, "staged", "archived")
                        self.logger.info("client rmulus ip lookups for '{}' with ingested_timestamps '{}' rotated from /staged/ to /archived/".format(db, currentLoadTstamps))
                    else:
                        self.logger.info('no new data to unload')
                else:
                    self.logger.info("rmulus table does not exist in db '{}'".format(db))
            except Exception as e:
                self.logger.error(e)
            #close connections to rmulus/clients cluster dbs
            rmulusConn.close()
            clientConn.close()
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: redshift_utils.py <cluster>")
        sys.exit(0)
    config = utils.load_config()
    utils.setup_logging()
    redshift_utils = RedshiftUtils(config, sys.argv[1])
