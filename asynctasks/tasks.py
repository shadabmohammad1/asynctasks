from __future__ import absolute_import, unicode_literals

import os
import requests
import json
import uuid
from datetime import datetime, timedelta


from celery_config import app
from celery.utils.log import get_task_logger


from django.core.mail import send_mail
from django.conf import settings


from asynctasks.utils import get_weight, get_dynamo_client, update_dynamo_entry_count


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
logger = get_task_logger(__name__)


@app.task
def user_ip_to_state_task(user_id,ip):
    from forum.user.models import UserProfile
    import urllib2
    import json
    userprofile = UserProfile.objects.filter(pk=user_id)
    url = 'http://ip-api.com/json/'+ip
    response = urllib2.urlopen(url).read()
    json_response = json.loads(response)
    userprofile.update(state_name = json_response['regionName'],city_name = json_response['city'])


def ffmpeg(*cmd):
    try:
        subprocess.check_output(['ffmpeg'] + list(cmd))
    except subprocess.CalledProcessError:
        return False
    return True

def upload_media(media_file,filename):
    try:
        client = boto3.client('s3',aws_access_key_id = settings.BOLOINDYA_AWS_ACCESS_KEY_ID,aws_secret_access_key = settings.BOLOINDYA_AWS_SECRET_ACCESS_KEY)
        filenameNext= str(filename).split('.')
        final_filename = str(filenameNext[0])+"."+str(filenameNext[1])
        client.put_object(Bucket=settings.BOLOINDYA_AWS_BUCKET_NAME, Key='watermark/' + final_filename, Body=media_file,ACL='public-read')
        filepath = "https://s3.amazonaws.com/"+settings.BOLOINDYA_AWS_BUCKET_NAME+"/watermark/"+final_filename
        return filepath
    except:
        return None

def create_downloaded_url(topic_id):
    import subprocess
    import os.path
    from datetime import datetime
    import os
    import boto3
    from django.conf import settings
    from forum.topic.models import Topic
    video_byte = Topic.objects.get(pk=topic_id)
    try:
        # print "start time: ", datetime.now()
        filename_temp = "temp_"+video_byte.backup_url.split('/')[-1]
        filename = video_byte.backup_url.split('/')[-1]
        cmd = ['ffmpeg','-i', video_byte.backup_url, '-vf',"[in]scale=540:-1,drawtext=text='@"+video_byte.user.username+"':x=10:y=H-th-20:fontsize=18:fontcolor=white[out]",settings.PROJECT_PATH+"/boloindya/scripts/watermark/"+filename_temp]
        ps = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        (output, stderr) = ps.communicate()
        cmd = 'ffmpeg -i '+settings.PROJECT_PATH+"/boloindya/scripts/watermark/"+filename_temp+' -ignore_loop 0 -i '+settings.PROJECT_PATH+"/boloindya/media/img/boloindya_white.gif"+' -filter_complex "[1:v]format=yuva444p,scale=140:140,setsar=1,rotate=0:c=white@0:ow=rotw(0):oh=roth(0) [rotate];[0:v][rotate] overlay=10:(main_h-overlay_h+10):shortest=1" -codec:a copy -y '+settings.PROJECT_PATH+"/boloindya/scripts/watermark/"+filename
        subprocess.call(cmd,shell=True)
        downloaded_url = upload_media(open(settings.PROJECT_PATH+"/boloindya/scripts/watermark/"+filename),filename)
        if downloaded_url:
            Topic.objects.filter(pk=video_byte.id).update(downloaded_url = downloaded_url,has_downloaded_url = True)
        if os.path.exists(settings.PROJECT_PATH+"/boloindya/scripts/watermark/"+filename):
            os.remove(settings.PROJECT_PATH+"/boloindya/scripts/watermark/"+filename_temp)
            os.remove(settings.PROJECT_PATH+"/boloindya/scripts/watermark/"+filename)
        # print "bye"
        # print "End time:  ",datetime.now()
    except Exception as e:
        try:
            os.remove(settings.PROJECT_PATH+"/boloindya/scripts/watermark/"+filename_temp)
        except:
            pass
        try:
            os.remove(settings.PROJECT_PATH+"/boloindya/scripts/watermark/"+filename)
        except:
            pass
        # print e

@app.task
def sync_contacts_with_user(user_id):
    from forum.user.models import UserProfile,UserPhoneBook,Contact
    try:
        user_phonebook = UserPhoneBook.objects.using('default').get(user_id=user_id)
        all_contact_no = list(user_phonebook.contact.all().values_list('contact_number',flat=True))
        all_userprofile = UserProfile.objects.filter(mobile_no__in=all_contact_no,user__is_active=True).values('mobile_no','user_id')
        if all_userprofile:
            for each_userprofile in all_userprofile:
                Contact.objects.filter(contact_number=each_userprofile['mobile_no']).update(is_user_registered=True,user_id=each_userprofile['user_id'])
    except:
        pass # No phonebook exist for user

@app.task
def cache_follow_post(user_id):
    from forum.topic.utils import update_redis_paginated_data, get_redis_vb_seen
    from forum.user.utils.follow_redis import get_redis_following
    from forum.user.models import UserProfile
    from forum.topic.models import Topic
    from django.db.models import Q
    all_seen_vb = []
    if user_id:
        all_seen_vb = get_redis_vb_seen(user_id)
    key = 'follow_post:'+str(user_id)
    all_follower = get_redis_following(user_id)
    category_follow = UserProfile.objects.get(user_id = user_id).sub_category.all().values_list('pk', flat = True)
    query = Topic.objects.filter(Q(user_id__in = all_follower)|Q(m2mcategory__id__in = category_follow, \
        language_id = UserProfile.objects.get(user_id = user_id).language), is_vb = True, is_removed = False, is_popular = False)\
        .exclude(pk__in = all_seen_vb).order_by('-id', '-vb_score')
    update_redis_paginated_data(key, query)

@app.task
def cache_popular_post(user_id,language_id):
    from forum.topic.utils import update_redis_paginated_data, get_redis_vb_seen
    from forum.user.utils.follow_redis import get_redis_following
    from forum.topic.models import Topic
    key = 'lang:'+str(language_id)+':popular_post:'+str(user_id)
    all_seen_vb= []
    if user_id:
        all_seen_vb = get_redis_vb_seen(user_id)
    query = Topic.objects.filter(is_vb = True, is_removed = False, language_id = language_id, is_popular = True)\
        .exclude(pk__in = all_seen_vb).order_by('-id', '-vb_score')
    update_redis_paginated_data(key, query)

@app.task
def create_topic_notification(created,instance_id):
    from forum.topic.models import Topic,Notification
    from forum.user.models import Follower
    from forum.user.utils.follow_redis import get_redis_follower
    try:
        instance = Topic.objects.get(pk=instance_id)
        if created:
            # all_follower_list = Follower.objects.filter(user_following = instance.user).values_list('user_follower_id',flat=True)
            all_follower_list = get_redis_follower(instance.user.id)
            for each in all_follower_list:
                notify = Notification.objects.create(for_user_id = each,topic = instance,notification_type='1',user = instance.user)
        instance.calculate_vb_score()
    except Exception as e:
        # print e
        pass

@app.task
def create_comment_notification(created,instance_id):
    from forum.topic.models import Notification
    from forum.comment.models import Comment
    from forum.user.models import Follower
    from forum.user.utils.follow_redis import get_redis_follower
    try:
        instance = Comment.objects.get(pk=instance_id)
        if created:
            # all_follower_list = Follower.objects.filter(user_following = instance.user).values_list('user_follower_id',flat=True)
            all_follower_list = get_redis_follower(instance.user.id)
            mentions_ids = get_mentions_and_send_notification(instance)
            for each in [user_id for user_id in all_follower_list if user_id not in mentions_ids]:
                if not str(each) == str(instance.topic.user.id):
                    notify = Notification.objects.create(for_user_id = each,topic = instance,notification_type='2',user = instance.user)
            if not instance.topic.user == instance.user:
                notify_owner = Notification.objects.create(for_user = instance.topic.user ,topic = instance,notification_type='3',user = instance.user)
    except Exception as e:
        # print e
        pass

from HTMLParser import HTMLParser
class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def get_mentions_and_send_notification(comment_obj):
    from django.contrib.auth.models import User
    from forum.topic.models import Notification
    comment = strip_tags(comment_obj.comment)
    mention_tag=[mention for mention in comment.split() if mention.startswith("@")]
    user_ids = []
    if mention_tag:
        for each_mention in mention_tag:
            try:
                user = User.objects.get(username=each_mention.strip('@'))
                user_ids.append(user.id)
                if not user == comment_obj.user:
                    notify_mention = Notification.objects.create(for_user = user  ,topic = comment_obj,notification_type='10',user = comment_obj.user)
            except Exception as e:
                # print e
                pass
    return user_ids
 
@app.task
def create_hash_view_count(create,instance_id):
    from forum.topic.models import Topic,TongueTwister,HashtagViewCounter
    from django.db.models import Sum
    from drf_spirit.utils import language_options
    try:
        for each_language in language_options:
            language_specific_vb = Topic.objects.filter(hash_tags__id=instance_id, is_removed=False, is_vb=True,language_id=each_language[0])
            language_specific_seen = language_specific_vb.aggregate(Sum('view_count'))
            language_specific_hashtag, is_created = HashtagViewCounter.objects.get_or_create(hashtag_id=instance_id,language=each_language[0])
            if language_specific_seen.has_key('view_count__sum') and language_specific_seen['view_count__sum']:
                # print "language_specific",each_language[1]," --> ",language_specific_seen['view_count__sum'],instance_id
                language_specific_hashtag.view_count = language_specific_seen['view_count__sum']
            else:
                language_specific_hashtag.view_count = 0
            language_specific_hashtag.video_count = len(language_specific_vb)
            language_specific_hashtag.save()
    except Exception as e:
        # print e
        pass

@app.task
def create_thumbnail_cloudfront(topic_id):
    try:
        from forum.topic.models import Topic
        from drf_spirit.utils import get_modified_url, check_url
        lambda_url = "http://boloindyapp-prod.s3-website-us-east-1.amazonaws.com/200x300"
        cloundfront_url = "http://d3g5w10b1w6clr.cloudfront.net/200x300"
        video_byte = Topic.objects.filter(pk=topic_id)
        if video_byte.count() > 0:
            thumbnail_url = video_byte[0].question_image
            lmabda_video_thumbnail_url = get_modified_url(thumbnail_url, lambda_url)
            response = check_url(lmabda_video_thumbnail_url)
            if response == '200':
                video_byte.update(is_thumbnail_resized = True)
                lmabda_cloudfront_url = get_modified_url(thumbnail_url, cloundfront_url)
                response = check_url(lmabda_cloudfront_url)
    except Exception as e:
        # print e
        pass

@app.task
def send_report_mail(report_id):
    dynamo_client = get_dynamo_client()

    report = dynamo_client.get_item(TableName="Report", Key={"id": {"S": report_id}}).get("Item")

    reporter = dynamo_client.get_item(TableName="User", 
                    Key={"id": {"S": report.get("reported_by_id").get("S")}}).get("Item")

    video_byte = dynamo_client.get_item(
            TableName="VideoByte", Key={"id": {"S": report.get("topic_id").get("S")}}).get("Item")

    if report.get("target_type").get("S") == "User":
        target_user_id = report.get("target_id").get("S")
    else:
        target_user_id = dynamo_client.get_item(TableName=report.get("target_type"),
                Key={"id": {"S": report.get("target_id").get("S")}}).get("Item").get("user_id").get("S")

    target_user = dynamo_client.get_item(TableName="User", Key={"id": {"S": target_user_id}}).get("Item")

    mail_message = render_to_string("report_email.html", context={
            "reported_by_username": reporter.get("username").get("S"),
            "reported_by_name": reporter.get("name").get("S"),
            "video_title": video_byte.get("title").get("S"),
            "video_id": video_byte.get("id").get("S"),
            "report_type": report.get("report_type").get("S"),
            "target_mobile": target_user.get("mobile_no").get("S"),
            "target_email": target_user.get("email").get("S")
        })

    requests.post(
            settings.MAILGUN_CONFIG.get("host"),
            auth={"api": settings.MAILGUN_CONFIG.get("token")},
            data={
                "from": settings.MAILGUN_CONFIG.get("from"),
                "to": settings.MAILGUN_CONFIG.get("to"),
                "cc": settings.MAILGUN_CONFIG.get("cc"),
                "bcc": settings.MAILGUN_CONFIG.get("bcc"),
                "subject": settings.MAILGUN_CONFIG.get("subject").format(target=report.get("target_type"), 
                                reporter_username=reporter.get("username").get("S")),
                "html": mail_message
            }
        )

    return True


def add_to_history(user_id, score, action, action_object_type, action_object_id, is_removed):
    dynamo_client = get_dynamo_client()

    last_evaluated_key = None
    history = None

    while True:
        result = dynamo_client.query(
            TableName="BoloActionHistory",
            IndexName="user_action_index",
            ExpressionAttributeValues={":u": {"S": user_id}, ":a": {"S": action}},
            KeyConditionExpression="user_id = :u and action = :a",
            LastEvaluatedKey=last_evaluated_key,
            AttributesToGet=["action_object_type", "action_object_id", "id"]
        )

        if result.get("Count") == 0:
            break

        history = filter(lambda x: x.get("action_object_type").get("S") == action_object_type  \
                    x.get("action_object_id").get("S") == action_object_id, result.get("Items"))

        if len(history):
            history = history[0]
            break

        last_evaluated_key = result.get("LastEvaluatedKey")

    if history:
        dynamo_client.update_item(
            TableName="BoloActionHistory",
            ExpressionAttributeNames={"#R": "is_removed"},
            ExpressionAttributeValues={":r": {"N": 1 if is_removed else 0}}
            Key={"id": {"S": history.get("id").get("S")}},
            UpdateExpression="SET #R = :r",
            ReturnValues="NONE"
        )
    else:
        dynamo_client.put_item(
            TableName="BoloActionHistory",
            Item={
                "id": str(uuid.uuid4()),
                "created_at": str(datetime.now()),
                "last_modified_at": str(datetime.now()),
                "user_id": user_id,
                "score": score,
                "action": action,
                "action_object_type": action_object_type,
                "action_object_id": action_object_id,
                "is_removed": 1 if is_removed else 0,
                "is_encashed": 0,
                "is_eligible_for_encash": 1
            }
        )


def add_bolo_score(user_id, feature, action_object_type, action_object_id):
    score = get_weight(feature)
    dynamo_client = get_dynamo_client()

    if not score:
        return

    update_dynamo_entry_count("User", bolo_score, int(score), {"id": {"S": user_id}}, dynamo_client)
    add_to_history(user_id, score, feature, action_object_type, action_object_id, False)

    if feature in ["create_topic", "create_topic_en"]:
        dynamo_client.put_item(
            TableName="Notification",
            Item={
                "id": str(uuid.uuid4())
                "created_at": str(datetime.now()),
                "last_modified_at": str(datetime.now())
                "for_user_id": user_id,
                "video_byte_type": action_object_type,
                "video_byte_id": action_object_id,
                "notification_type": "8",
                "user_id": user_id
            }
        )


@app.task
def default_boloindya_follow(user_id, language):
    dynamo_client = get_dynamo_client()

    user = dynamo_client.get_item(TableName="User", Key={"id": {"S": user_id}}).get("Item")

    language_name = settings.LANGUAGE_OPTIONS_DICT.get(language)
    boloindya_username = "boloindya_%s"%language_name.lower() if language_name else "boloindya"

    boloindya_user = dynamo_client.query(
        TableName="User",
        IndexName="username_index",
        AttributesToGet=["id", "username"],
        ExpressionAttributeValues={":username": {"S": boloindya_username}},
        KeyConditionExpression="username = :username"
    ).get("Items")[0]

    try:
        follow = dynamo_client.update_item(
            TableName="Follow",
            ExpressionAttributeNames={"#A": "is_active"},
            ExpressionAttributeValues={":a": {"N": 1}},
            Key={
                "user_id": {"S": boloindya_user.get("id").get("S")},
                "follower_id": {"S": user_id}
            },
            UpdateExpression="SET #A = :a",
            ReturnValues="UPDATED_NEW"
        )

    except Exception as e:
        follow = dynamo_client.put_item(
            TableName="Follow",
            Item={
                "user_id": {"S": boloindya_user.get("id").get("S")},
                "follower_id": {"S": user_id},
                "is_active": {"N", 1},
                "created_at": {"S", str(datetime.now())},
                "last_modified_at": {"S", str(datetime.now())}
            },
            ReturnValues="UPDATED_NEW"
        ).get("Attributes")

        add_bolo_score(user_id, 'follow', 'Follow', 
                "%s:%s"%(follow.get("user_id").get("S"), follow.get("follower_id").get("S")))

    update_dynamo_entry_count("User", "follow_count", 1, {"id": {"S": user_id}}, dynamo_client)
    update_dynamo_entry_count("User", "follower_count", 1, {"id": {"S": follow.get("follower_id").get("S")}}, dynamo_client)

    #To do: Update these functions too
    update_redis_following(user.id, bolo_indya_user.id,True)
    update_redis_follower(bolo_indya_user.id,user.id,True)
    update_profile_counter(user.id,'follower_count',1, True)
    update_profile_counter(bolo_indya_user.id,'follow_count',1, True)



@app.task
def send_upload_video_notification(data):
    headers = {
        'Authorization': 'Bearer ' + _get_access_token(), 
        'Content-Type': 'application/json; UTF-8' }

    devices = get_dynamo_client().query(
                TableName="FCMDevice", 
                IndexName="user_install_index",
                AttributesToGet=["user_id", "is_uninstalled", "reg_id"],
                ExpressionAttributeValues={
                    ":user_id": {"S": data.get("particular_user_id", None)},
                    ":is_uninstalled": {"N": 0}
                },  
                KeyConditionExpression="user_id = :user_id and is_uninstalled  = :is_uninstalled"
            )

    for device in devices.get("Items"):
        fcm_message = {
            "message": {
                "token": device.get("reg_id"),
                "data": {
                    "title_upper": data.get("upper_title", ""), 
                    "title": data.get("title", ""), 
                    "id": data.get("id", ""), 
                    "type": data.get("notification_type", ""),
                    "notification_id": "-1", 
                    "image_url": data.get("image_url", "")
                }
            }
        }

        response = requests.post(
            settings.FCM_CONFIG.get("message_url"), 
            data=json.dumps(fcm_message), 
            headers=headers
        )


if __name__ == '__main__':
    app.start()
