"""
EFK handler Lambda function

"""

#region  imports
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
import logging
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")
#endregion 


#region  variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)
indicesToBackup = []
currDateTime= datetime.now()
#endregion

#region  constants which are read from events
userName =''
secret = ''
daysCount = 7
host = ''
port = ''
snapShotReposName = ''
snapShotName = ''
repoType='s3'
bucketId = ''
repoLocation =''
deleteIndices = True
deleteOldSnapShots = True
takeSnapShot = True
createNewRepository = True
#endregion

#region  RepoCreationBodyTemplate
createReposBody = {
    "type" : '',
    "settings" : {
      "bucket" : '',
      'location':'',
      "server_side_encryption" : "true",
      "compress" : "true"
    }
}
#endregion

#region createRepoHelper
"""
def CreateNewRepoBody()-> any:
    '''
        create body for creating new repository
    '''
    global createReposBody
    global repoType
    createReposBody['type']= repoType.strip()
    if (repoType.strip() == 's3'):
        global bucketId
        createReposBody['settings']['bucket'] = bucketId
        del createReposBody['settings']['location']
    else :
        global repoLocation
        createReposBody['settings']['location'] = repoLocation
        del createReposBody['settings']['bucket']
    return createReposBody"""
#endregion

#region CreateRepository
"""
def CreateRepository(es:Elasticsearch)->bool :
    '''
        creates new repository
    '''
    global logger
    try:
        body = CreateNewRepoBody()
        nodes = es.snapshot.create_repository(repository=snapShotReposName , body=body )
        logger.info(nodes)
        return True
    except Exception as ex:
        logger.exception(ex)
        return False
    finally :
        logger. info("completed of CreateRepository")
"""
#endregion

#region  snapShotTemplate

createSnapShotBody = {
        "indices": '',
        "ignore_unavailable": True,
        "include_global_state": False,
        "metadata": {
            "taken_by": "efk_lambda",
            "taken_because": "scheduled lambda snapshot on  " +currDateTime.strftime("%Y.%m.%d.%H") 
        }
    } 
#endregion

#region  updateConstants
def updateConstants(event):
    '''
        updates the constants from the event

        userName = elastic user name
        secret = elastic search password
        daysCount = day count for the older logs
        host =  host address of elasticsearch
        port =  port address of elasticsearch
        snapShotReposName = custom name to the snapshot repository for create or take/delete snapshots
        snapShotName = custom snapshotname to delete/create
        repoType= snapshot repository type in create
        bucketId =  s3 bucket id
        deleteIndices = true/false
        deleteOldSnapShots = True/false
        takeSnapShot = True/false
        createNewRepository = True/false
    '''
    global logger
    try:
        global userName
        userName = event['userName'] if ('userName' in event) else userName
        global secret
        secret = event['secret'] if ('secret' in event) else secret
        global host
        host = event['host'] if ('host' in event) else host
        global port
        port = event['port'] if ('port' in event) else port
        global snapShotReposName
        snapShotReposName = event['snapShotReposName'] if ('snapShotReposName' in event) else snapShotReposName
        global repoType
        repoType = event['repoType'] if ('repoType' in event) else repoType
        global snapShotName
        snapShotName = event['snapShotName'] if ('snapShotName' in event) else snapShotName
        global bucketId
        bucketId = event['bucketId'] if ('bucketId' in event) else bucketId
        global repoLocation
        repoLocation = event['repoLocation'] if ('repoLocation' in event) else repoLocation
        global deleteIndices
        deleteIndices =event['deleteIndices'] if ('deleteIndices' in event) else deleteIndices
        global deleteOldSnapShots
        deleteOldSnapShots =event['deleteOldSnapShots'] if ('deleteOldSnapShots' in event) else deleteIndices
        global takeSnapShot
        takeSnapShot =event['takeSnapShot'] if ('takeSnapShot' in event) else takeSnapShot
        global createNewRepository
        createNewRepository =event['createNewRepository'] if ('createNewRepository' in event) else createNewRepository
        global daysCount
        daysCount = event['daysCount'] if ('daysCount' in event) else daysCount
        global currDateTime
        CalcDate = currDateTime - timedelta(daysCount)
        result = ((userName,secret),CalcDate)
    except Exception as ex:
        logger.exception(ex)
        result = ('','')
    finally :
        logger. info("completed of updateConstants")
    return result 

#endregion

#region deleteSnapshot helper
def DeleteSnapShot(es : Elasticsearch, snapShotName:str, repositoryName:str)->bool:
    '''
        deletes snapshot snapShotName from teh repositoryName
    '''
    try:
        result = es.snapshot.delete(repository=repositoryName,snapshot=snapShotName)
        return True
    except Exception   as ex:
        logger.exception(ex)
        return False
    
#endregion

#region deleteOlderSnapShots
def deleteOlderSnapShots(es :Elasticsearch ,calcDate : datetime )-> bool :
    '''
        deletes older snapshots from the repository based on the snapshot creation end time
    '''
    global logger
    global snapShotReposName
    try:
        allSnapShots = es.snapshot.get(repository=snapShotReposName,snapshot="*",ignore_unavailable=True,format="json")
        if 'snapshots' not in allSnapShots :
            return False
        for snapShot in allSnapShots['snapshots']:
            if(snapShot['end_time_in_millis'] < datetime.timestamp(calcDate)) :
                DeleteSnapShot(snapShot['snapshot'] , snapShotReposName )
        return True
    except Exception as ex:
        logger.exception(ex)
        return False
    finally:
        logger. info("completed of deleteOlderSnapShots")
#endregion

#region createSnapShotHelpers
def createSnapShotReq()->any:
    '''
        creates New snapshot body 
    '''
    global createSnapShotBody
    global indicesToBackup
    createSnapShotBody['indices'] = ','.join(indicesToBackup)
    return createSnapShotBody
#endregion

#region takeNewSnapShot
def takeNewSnapShot(es:Elasticsearch) -> bool:
    global logger
    global snapShotReposName
    global snapShotName
    global currDateTime
    try:
        dateHour = currDateTime.strftime("%Y.%m.%d.%H") 
        es.snapshot.create(snapShotReposName,snapShotName+dateHour,body=createSnapShotReq())
        return True
    except Exception as ex:
        logger.exception(ex)
        return False
    finally:
        logger. info("completed of takeNewSnapShot")
#endregion

#region HelperDeleteIndex
def JoinToSnapShotIndices(index:str)->bool:
    '''
        Joins the index to snapshot index list
    '''
    global indicesToBackup
    indicesToBackup.append(index)
    return True

def DeleteIndex(es:Elasticsearch, index:str)->bool :
    '''
        deletes index from the cluster
    '''
    try:
        result = es.indices.delete(index,allow_no_indices=True, ignore_unavailable=True)
    except Exception as ex:
        pass
    return True

def TestForOldIndex(index:any ,calcDate : datetime )->bool:
    '''
        checks if index is old enough
    '''
    result = False
    try:
        name =  str(index['index'])
        indexDate = datetime.strptime(name.split('-')[-1] , "%Y.%m.%d")
        if (indexDate < calcDate):
            result = True
        return result
    except Exception as ex:
        return False
 #endregion   

#region DeleteOldIndices
def deleteOldIndices(es: Elasticsearch, calcDate :datetime) -> bool:
    '''
        gets all indices and deletes all older indices
    '''
    global logger
    try:
        allindices =  es.cat.indices(format="json")
        for index in allindices :
            name = index['index']
            if name.startswith('.'):
                continue
            if TestForOldIndex(index,calcDate):
                logger.info("Deleting Index "+name +" : " + str(DeleteIndex(es,name)) )
            else :
                logger.info("Adding To SnapShot Index "+name +" : " + str(JoinToSnapShotIndices(name)) )
        return True
    except Exception as ex:
        logger.exception(ex)
        return False
    finally:
        logger. info("completed of deleteOldIndices")
#endregion

#region  Efk_Handler
def efk_handler(event, context):
    '''
        efk hanlder which performs tasks based on the event provided
        200 : success
        500 : failure
    '''
    global logger
    global deleteIndices
    global deleteOldSnapShots
    global takeSnapShot
    try:
        auth,calcDate = updateConstants(event)    
        es = Elasticsearch(host, http_auth=auth, use_ssl=True,port=port,verify_certs=False)
        logger.info(es.info())
        result = {
            "OlderIndicesDeleted" : deleteOldIndices(es,calcDate) if deleteIndices else deleteIndices,
            "OlderSnapshotsDeleted":deleteOlderSnapShots(es,calcDate) if deleteOldSnapShots else deleteOldSnapShots,
            #"NewRepositoryCreated": CreateRepository(es) if createNewRepository else createNewRepository,
            "TakenNewSnapShot" : takeNewSnapShot(es) if takeSnapShot else takeSnapShot
        }
        logger.info(result)
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
        return result
    except Exception as ex:
        logger.exception(ex)
        return {
            'statusCode': 500,
            'body': json.dumps(ex)
        } 
    finally:
        logger. info("completed of efk_handler")
    
   
#endregion 

#region testEvent
"""testEvent = {
    "userName" : "elastic",
    "secret" : "itFUWOYAXf1x1a3AdR8macZh",
    "daysCount" : 4,
    "host" : "02c5bc696f5844f681cf5bfca6c5e9a6.us-east-1.aws.found.io",
    "port" : "9243",
    "snapShotReposName" : "found-snapshots",
    "snapShotName" : "efkbackup",
    "repoType":"s3",
    "bucketId" : "2c873ed81d5e479fbc1a715e75361039",
    'repoLocation' :'',
    "deleteIndices" : True,
    "deleteOldSnapShots" : True,
    "takeSnapShot" : True,
    "createNewRepository" : True
}

efk_handler(testEvent,None)"""
#endregion