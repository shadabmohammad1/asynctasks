

from rest_framework.generics import GenericAPIView


from django.utils.module_loading import import_string
from django.http import JsonResponse


app_label = "asynctasks"



class AddToQueueView(GenericAPIView):
    def post(self, request, *args, **kwargs):
        print(" request method == ", request.method)
        print("data- -- ", request.data)
        for celery_function in request.data.get('celery_functions', []):
            func = import_string("%s.tasks.%s" %(app_label, celery_function.get('name')))
            print(" func == = =", func)
            func(*celery_function.get('args'), **celery_function.get('kwargs'))

        return JsonResponse({"status": "success"})