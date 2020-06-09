import requests, json, re, os, sys, urllib.request, boto3, logging
from botocore.exceptions import ClientError
from pandas.io.json import json_normalize
from moviepy.editor import *

clientID = "{TWITCH_CLIENT_ID}"
clientSecret = '{TWITCH_CLIENT_SECRET}'
redirectURI = '{TWITCH_CLIENT_REDIRECT_URI}'
auddioAPIKey = '{AUDD.IO API KEY}' #https://audd.io
basepath = 'downloads/'
aws_acess_key = '{AWS ACESS KEY FOR S3}'
aws_secret = '{AWS SECRET KEY}'
os.environ["AWS_ACCESS_KEY_ID"] = aws_acess_key
os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret


def getAccessToken():
    cc = 'client_credentials'
    sendURL = 'https://id.twitch.tv/oauth2/token?client_id={}&client_secret={}&grant_type={}&scope=clips:edit'.format(
        clientID, clientSecret, cc)
    r = requests.post(sendURL)
    tokenResponse = r.json()
    tokenResponse = json.dumps(tokenResponse)
    tokenResponse = json.loads(tokenResponse)
    access_token = tokenResponse['access_token']
    return(access_token)

def getUserID(accessToken, broadcaster):
    headers = {'Client-ID': clientID, 'Authorization': 'Bearer {}'.format(accessToken)}
    r = requests.get('https://api.twitch.tv/helix/users?login={}'.format(broadcaster), headers=headers)
    userData = r.json()
    userData = json.dumps(userData)
    userData = json.loads(userData)
    userData = json_normalize(userData, record_path='data')
    userID = int(userData['id'])
    return(userID)


def authValidate(accessToken): #Code unnecessary at this moment
    authURL = 'https://id.twitch.tv/oauth2/validate'
    headers = 'Authorization: Bearer {}'.format(accessToken) #variable should be access token


def getClips(accessToken, userID):
    reqNumber = 0
    headers = {'Client-ID' : clientID, 'Authorization' : 'Bearer {}'.format(accessToken)}
    #print(accessToken)
    while reqNumber == 0:
        r = requests.get('https://api.twitch.tv/helix/clips?broadcaster_id={}&first=100'.format(userID), headers = headers)
        clipIDS = r.json()
        clipIDS = json.dumps(clipIDS)
        clipIDS = json.loads(clipIDS)
        ids = json_normalize(clipIDS, record_path='data')
        page = clipIDS['pagination']['cursor']
        reqNumber = reqNumber + 1
    while 1 <= reqNumber <= 10:
        requesturl = 'https://api.twitch.tv/helix/clips?broadcaster_id={}&first=100&after={}'.format(userID, page)
        r = requests.get(requesturl, headers=headers)
        clipIDS = r.json()
        clipIDS = json.dumps(clipIDS)
        clipIDS = json.loads(clipIDS)
        idtemp = json_normalize(clipIDS, record_path='data')
        ids = ids.append(idtemp)
        try:
            page = clipIDS['pagination']['cursor']
        except:
            #print(ids['url'])
            return(ids)
        reqNumber = reqNumber + 1
    else:
        print(ids)

def dl_progress(count, block_size, total_size):
    percent = int(count * block_size * 100 / total_size)
    sys.stdout.write("\r...%d%%" % percent)
    sys.stdout.flush()

def getMP4Data(clipsdata):
    for i in range(len(clipsdata.index)):
        thumbUrl = clipsdata.iloc[i,13]
        title = clipsdata.iloc[i,0]
        slicePoint = thumbUrl.index("-preview-")
        mp4url = thumbUrl[:slicePoint] + '.mp4'
        regex = re.compile('[^a-zA-Z0-9_]')
        title = title.replace(' ', '_')
        out_filename = regex.sub('', title) + '.mp4'
        output_path = (basepath + out_filename)

        urllib.request.urlretrieve(mp4url, output_path, reporthook=dl_progress)
        urllib.request.urlcleanup()
        video = VideoFileClip(output_path)
        video.audio.write_audiofile('downloads/{}.mp3'.format(title))
        del video.reader
        del video
        os.remove(output_path)
        output_path = 'downloads/{}.mp3'.format(title)

        upload_file(output_path, '{S3 BUCKET NAME}', object_name=title)
        #return(output_path, title)

def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name

        # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        #s3 = boto3.resource('s3')
        #object = s3.Bucket(bucket).Object(file_name)
        #object.Acl().put(ACL='public-read')
    except ClientError as e:
        logging.error(e)
        return False
    return True

def get_music_data():
    client = boto3.client("s3")

    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket="{S3 BUCKET NAME}"):
        #print(page["Contents"])
        contents = json_normalize(page["Contents"])
        #print(contents)
        keys = contents["Key"]
        for i in range(len(keys.index)):
            link = "http://{S3 BUCKET NAME}.s3.amazonaws.com/{}".format(keys[i])
            #print(link)
            status = send_to_audd(link)

            copyrightinfo = json_normalize(status)
            try:
                print("clips.twitch.tv/{} has copyrighted content by {}, including song {} by {}. This clip must be removed in order to"
                      " abide by the DMCA".format(contents.iloc[i, 0],
                                             copyrightinfo["result.label"].values[0], copyrightinfo["result.title"].values[0], copyrightinfo["result.artist"].values[0]))
            except:
                pass


def send_to_audd(link):
    data = {
        'url': link,
        'return': 'apple_music,spotify',
        'api_token': auddioAPIKey
    }
    result = requests.post('https://api.audd.io/', data=data)
    result = result.json()
    result = json.dumps(result)
    result = json.loads(result)
    return(result)

if __name__ == '__main__':
    twitch_user = input("who would you like to scan for DMCA content for: ")
    print("WARNING this may take a while as it will download up to 1100 clips")
    accessToken = getAccessToken()
    userID = getUserID(accessToken, twitch_user)
    twitchids = getClips(accessToken, userID)
    filepath, title = getMP4Data(twitchids)
    get_music_data()

