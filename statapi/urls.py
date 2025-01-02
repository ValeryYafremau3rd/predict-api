from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path('', include('teamsapp.urls')),
    path('', include('matchesapp.urls')),
    path('', include('leagues.urls')),
    path('', include('strategy.urls')),
    path('', include('builder.urls')),
    path('admin/', admin.site.urls),
]