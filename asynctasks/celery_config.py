from __future__ import absolute_import, unicode_literals
from celery import Celery
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings_local")

redis_host = 'boloindya-prod.quyamn.ng.0001.apne1.cache.amazonaws.com'
app = Celery('boloindya',
             broker = 'redis://' + redis_host + ':6379',
             backend = 'redis://' + redis_host + ':6379',
             include=['tasks'])

if __name__ == '__main__':
    app.start()
