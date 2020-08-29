

config = {
    'rds_config': {
        'database': 'boloindya',
        'user': 'boloindya',
        'password': 'bng321',
        'host': 'localhost',
        'port': '5433',  
    },
    'dynamo_config': {
        # 'aws_access_key_id': '',
        # 'aws_secret_access_key': '',
        # 'region_name': 'ap-south-1',
        'endpoint_url': 'http://54.238.171.138:8000'
    },
    'redis_config': {
        'host': 'localhost',
        'port': 6379,
        'db': 0
    },
    'table': [{
        'rds_query': {
            'select': 'auth_user.id, auth_user.id as rds_id, auth_user.password, auth_user.last_login, case when auth_user.is_superuser then 1 else 0 end as is_superuser, auth_user.username, auth_user.first_name, auth_user.last_name, auth_user.email, case when auth_user.is_staff then 1 else 0 end as is_staff, case when auth_user.is_active then 1 else 0 end as is_active, auth_user.date_joined, forum_user_userprofile.id as profile_id, forum_user_userprofile.slug, forum_user_userprofile.location, forum_user_userprofile.last_seen, forum_user_userprofile.last_ip, forum_user_userprofile.timezone, case when forum_user_userprofile.is_administrator then 1 else 0 end as is_administrator, case when forum_user_userprofile.is_moderator then 1 else 0 end as is_moderator, case when forum_user_userprofile.is_verified then 1 else 0 end as is_verified, forum_user_userprofile.topic_count, forum_user_userprofile.comment_count,  forum_user_userprofile.last_post_hash, forum_user_userprofile.last_post_on, forum_user_userprofile.about, forum_user_userprofile.bio, forum_user_userprofile.extra_data, forum_user_userprofile.language, forum_user_userprofile.name, forum_user_userprofile.profile_pic, forum_user_userprofile.refrence, forum_user_userprofile.social_identifier, forum_user_userprofile.answer_count, forum_user_userprofile.bolo_score, forum_user_userprofile.follow_count, forum_user_userprofile.follower_count, forum_user_userprofile.like_count, forum_user_userprofile.question_count, forum_user_userprofile.share_count, forum_user_userprofile.mobile_no, case when forum_user_userprofile.is_geo_location then 1 else 0 end as is_geo_location, forum_user_userprofile.lang, forum_user_userprofile.lat, forum_user_userprofile.click_id, forum_user_userprofile.click_id_response, forum_user_userprofile.is_test_user, forum_user_userprofile.vb_count, case when forum_user_userprofile.is_expert then 1 else 0 end as is_expert, forum_user_userprofile.d_o_b, forum_user_userprofile.gender, forum_user_userprofile.view_count, forum_user_userprofile.linkedin_url, forum_user_userprofile.instagarm_id, forum_user_userprofile.twitter_id, forum_user_userprofile.encashable_bolo_score, case when forum_user_userprofile.is_dark_mode_enabled then 1 else 0 end as is_dark_mode_enabled, case when forum_user_userprofile.is_business then 1 else 0 end as is_business, case when forum_user_userprofile.is_popular then 1 else 0 end as is_popular, case when forum_user_userprofile.is_superstar then 1 else 0 end as is_superstar, forum_user_userprofile.cover_pic, forum_user_userprofile.total_time_spent, forum_user_userprofile.total_vb_playtime, forum_user_userprofile.own_vb_view_count, forum_user_userprofile.city_name, forum_user_userprofile.state_name, forum_user_userprofile.paytm_number, forum_user_userprofile.android_did, case when forum_user_userprofile.is_guest_user then 1 else 0 end as is_guest_user',
            'from': 'auth_user left join forum_user_userprofile on forum_user_userprofile.user_id = auth_user.id',
            'where': 'true',
            'limit': 2000,
        },
        'rds_table_name': 'auth_user',
        'dynamo_table_name': 'User',
        'redis_save_list' : [{
                'prefix': 'user:',
                'key': 'rds_id',
                'assign': 'id'
            }],
        'is_active': False,
        'elasticsearch': {
            'keys': ['id', 'name', 'username', 'email', 'is_popular', 'is_active', 
                        'date_joined', 'follower_count'],
            'index': 'user-index'
        }
    },{
        'rds_query': {
            'select': 'id as rds_id, id, created_at, last_modified, case when is_active then 1 else 0 end as is_active, user_follower_id as follower_id, user_following_id as user_id',
            'from': 'forum_user_follower',
            'where': 'true',
            'limit': 2000
        },
        'rds_table_name': 'forum_user_follower',
        'dynamo_table_name': 'Follower',
        'is_active': False,
        'redis_get_list': [{
            'key': 'follower_id',
            'redis_key': 'user:%s'
        },{
            'key': 'user_id',
            'redis_key': 'user:%s'
        }]
    },{
        'rds_query': {
            'select': 'id as rds_id, id, created_at, last_modified, features, weight, bolo_score, "equivalent_INR", is_monetize',
            'from': 'forum_user_weight',
            'where': 'true',
            'limit': 1000
        },
        'rds_table_name': 'forum_user_weight',
        'dynamo_table_name': 'Weight',
        'is_active': False
    }, {
        'rds_query': {
            'select': 'id, id as rds_id, title, slug, description, is_closed, is_removed, is_private, parent_id, is_global, color, reindex_at, category_image, order_no, is_engagement, view_count, dark_category_image',
            'from': 'forum_category_category',
            'where': 'true',
        },
        'rds_table_name': 'forum_category_category',
        'dynamo_table_name': 'Category',
        'is_active': False,
        'redis_save_list' : [{
            'prefix': 'category:',
            'key': 'rds_id', #RDS id
            'assign': 'id' # Dynamo UUID
        }],
        'mark_migrate': False
    }, {
        'rds_query': {
            'select': 'id, now() as created_at, id as category_id, language, title',
            'from': """(
                            select id, (regexp_split_to_array('hindi_title', '_')::varchar[])[1] as language,hindi_title as title from forum_category_category
                            union
                            select id, (regexp_split_to_array('tamil_title', '_')::varchar[])[1] as language,  tamil_title as title from forum_category_category
                            union
                            select id, (regexp_split_to_array('telgu_title', '_')::varchar[])[1] as language,  telgu_title as title from forum_category_category
                            union
                            select id, (regexp_split_to_array('bengali_title', '_')::varchar[])[1] as language,  bengali_title as title from forum_category_category
                            union
                            select id, (regexp_split_to_array('kannada_title', '_')::varchar[])[1] as language,  kannada_title as title from forum_category_category
                            union
                            select id, (regexp_split_to_array('gujrati_title', '_')::varchar[])[1] as language,  gujrati_title as title from forum_category_category
                            union
                            select id, (regexp_split_to_array('malayalam_title', '_')::varchar[])[1] as language,  malayalam_title as title from forum_category_category
                            union
                            select id, (regexp_split_to_array('marathi_title', '_')::varchar[])[1] as language,  marathi_title as title from forum_category_category
                            union
                            select id, (regexp_split_to_array('odia_title', '_')::varchar[])[1] as language,  odia_title as title from forum_category_category
                            union
                            select id, (regexp_split_to_array('punjabi_title', '_')::varchar[])[1] as language,  punjabi_title as title from forum_category_category) as A""",
            'where': 'true',
        },
        'rds_table_name': 'forum_category_category',
        'mark_migrate': False,
        'dynamo_table_name': 'CategoryLanguage',
        'redis_get_list': [{
            'key': 'category_id',
            'redis_key': 'category:%s'
        }],
        'is_active': False
    }, {
        'rds_query': {
            'select': 'auth_user.id as user_id, forum_user_userprofile_sub_category.category_id',
            'from': 'forum_user_userprofile_sub_category inner join forum_user_userprofile on forum_user_userprofile.id = forum_user_userprofile_sub_category.userprofile_id inner join auth_user on auth_user.id = forum_user_userprofile.user_id',
            'where': 'true',
            'limit': 2000
        },
        'rds_table_name': 'forum_user_userprofile_sub_category',
        'dynamo_table_name': 'UserCategory',
        'redis_get_list': [{
            'key': 'category_id',
            'redis_key': 'category:%s'
        }, {
            'key': 'user_id',
            'redis_key': 'user:%s'
        }],
        'is_active': False
    }, {
        'rds_query': {
            'select': 'id, id as rds_id, created_at, last_modified, is_active, duration_start_date, duration_end_date, bolo_score_earned, is_encashed, enchashed_on, is_eligible_for_encash, user_id, bolo_score_details, "equivalent_INR", encashable_cycle, is_expired',
            'from': 'forum_payment_encashabledetail',
            'where': 'true',
            'limit': 2000
        },
        'rds_table_name': 'forum_payment_encashabledetail',
        'dynamo_table_name': 'EncashableDetail',
        'is_active': False,
        'redis_get_list': [{
            'key': 'user_id',
            'redis_key': 'user:%s'
        }]
    }, {
        'rds_query': {
            'select': 'id , id as rds_id, user_id, title, created_at, last_modified, question_video, slug, language_id, question_image, is_popular, media_duration, is_removed, thumbnail, view_count, imp_count, comment_count, total_share_count, share_count, is_vb, likes_count, is_monetized, is_moderated, vb_width, vb_height, likes_count as video_like_count, total_share_count as video_share_count, is_thumbnail_resized, whatsapp_share_count, linkedin_share_count, facebook_share_count, twitter_share_count, backup_url, vb_playtime, vb_score, is_boosted, boosted_till, boosted_start_time, boosted_end_time, is_logo_checked, time_deleted, plag_text',
            'from': 'forum_topic_topic',
            'where': 'true',
            'limit': 100
        },
        'rds_table_name': 'forum_topic_topic',
        'dynamo_table_name': 'VideoByte',
        'redis_save_list': [{
            'prefix': 'video:',
            'key': 'rds_id', #RDS id
            'assign': 'id' # Dynamo UUID
        }],
        'redis_get_list': [{
            'key': 'user_id',
            'redis_key': 'user:%s'
        }],
        'is_active': True,
        'elasticsearch': {
            'keys': ['user_id', 'title', 'created_at', 'media_duration', 'thumbnail', 'language_id', 'views', 'is_moderated', 'is_monetized', 'is_popular'],
            'index': 'video-byte-index'
        }
    }]

}


