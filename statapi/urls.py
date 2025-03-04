from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path('', include('teamsapp.urls')),
    path('matches/', include('matchesapp.urls')),
    path('leagues/', include('leaguesapp.urls')),
    path('predicted/', include('predictedapp.urls')),
    path('events/', include('eventapp.urls')),
    path('group/', include('groupapp.urls')),
    path('queue/', include('queueapp.urls')),
    path('admin/', admin.site.urls),
]