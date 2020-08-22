"""
Django settings for asynctasks project.

Generated by 'django-admin startproject' using Django 2.1.

For more information on this file, see
https://docs.djangoproject.com/en/2.1/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/2.1/ref/settings/
"""

import os

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/2.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '=iz67n+1f9-tf2$@2hne*)y+dmxx420ouu4nr@&v+5ajnwodrg'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'asynctasks'
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'asynctasks.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'asynctasks.wsgi.application'


# Database
# https://docs.djangoproject.com/en/2.1/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
    }
}


# Password validation
# https://docs.djangoproject.com/en/2.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/2.1/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/2.1/howto/static-files/

STATIC_URL = '/static/'


FCM_CONFIG = {
    "message_url": "https://fcm.googleapis.com/v1/projects/boloindya-1ec98/messages:send",
    "file_path": "boloindya-1ec98-firebase-adminsdk-ldrqh-27bdfce28b.json",
    "auth_url": "https://www.googleapis.com/auth/firebase.messaging"
}


LANGUAGE_OPTIONS = (
    ('1', "English", "en"),
    ('2', "Hindi", "hi"),
    ('3', "Tamil", "ta"),
    ('4', "Telugu", "te"),
    ('5', "Bengali", "bn"),
    ('6', "Kannada", "kn"),
    ('7', "Malayalam", "ml"),
    ('8', "Gujrati", "gu"),
    ('9', "Marathi", "mr"),
    ('10', "Punjabi", "pa"),
    ('11', "Odia", "or")

)

LANGUAGE_OPTIONS_DICT = {item[0]: item  for item in LANGUAGE_OPTIONS}


MAILGUN_CONFIG = {
    "host": "https://api.mailgun.net/v3/mail.careeranna.com/messages",
    "token": "d6c66f5dd85b4451bbcbd94cb7406f92-bbbc8336-97426998",
    "to": ["support@boloindya.com"],
    "cc": [],
    "bcc": ["anshika@careeranna.com", "maaz@careeranna.com", "ankit@careeranna.com", 
            "gitesh@careeranna.com", "tanmai@boloindya.com"],
    "from": "BoloIndya Support <support@boloindya.com>",
    "subject": "BoloIndya Report Received | {target} | {reporter_username}"
}
