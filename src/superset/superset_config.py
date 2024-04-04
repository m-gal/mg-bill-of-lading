# CUSTOM CONFIGURATIONS FOR APACHE SUPERSET

import os
from datetime import timedelta


# # ------------------------------------------------------------------------------
# # * Path to your Superset project folder
# # ------------------------------------------------------------------------------
# SUPERSET_DIR = os.path.abspath("D:/dprojects/mgal-for-github/mg-bill-of-lading/src/superset")


# # ------------------------------------------------------------------------------
# # * Superset specific config
# # ------------------------------------------------------------------------------
# ROW_LIMIT = 50000
# VIZ_ROW_LIMIT = 10000
# # max rows retrieved when requesting samples from datasource in explore view
# SAMPLES_ROW_LIMIT = 1000
# # max rows retrieved by filter select auto complete
# FILTER_SELECT_ROW_LIMIT = 10000

# SUPERSET_WEBSERVER_PORT = 8088


# # ------------------------------------------------------------------------------
# # * Keys config
# # ------------------------------------------------------------------------------
# # Your App secret key
# SECRET_KEY = "b'VVlwnJ7Scpg_6yWQyaJNketlg7K4Ypylvvr5mHMx1qs='"

# # The SQLAlchemy connection string.
# # SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(SUPERSET_DIR, "superset-mgbol.db")
# SQLALCHEMY_DATABASE_URI = 'postgresql://superset:superset@localhost/mg-bol-superset'

# # Set this API key to enable Mapbox visualizations
# MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY")


# # ------------------------------------------------------------------------------
# # * Cache config
# # ------------------------------------------------------------------------------
# # Default cache timeout, applies to all cache backends unless specifically overridden in
# # each cache config.
# CACHE_DEFAULT_TIMEOUT = int(timedelta(days=100).total_seconds())

# # Default cache for Superset objects
# # CACHE_CONFIG: CacheConfig = {"CACHE_TYPE": "null"}
# CACHE_CONFIG = {
#     "CACHE_TYPE": "filesystem",
#     "CACHE_DEFAULT_TIMEOUT": CACHE_DEFAULT_TIMEOUT,
#     "CACHE_DIR": os.path.join(SUPERSET_DIR, "superset-cache"),
# }

# # Cache for datasource metadata and query results
# # DATA_CACHE_CONFIG: CacheConfig = {"CACHE_TYPE": "null"}
# DATA_CACHE_CONFIG = {
#     "CACHE_TYPE": "filesystem",
#     "CACHE_DEFAULT_TIMEOUT": CACHE_DEFAULT_TIMEOUT,
#     "CACHE_DIR": os.path.join(SUPERSET_DIR, "superset-cache"),
# }


# ------------------------------------------------------------------------------
# * * * * * * * * * * * *  SUPERSET  HEROKU  CONFIG  * * * * * * * * * * * * * *
# When want to run the Superset dashboard on Heroku:
# 1. Comment all above
# 2. Uncomment all followings
# ------------------------------------------------------------------------------
ROW_LIMIT = 10000

# for it to work in heroku basic/hobby dynos increase as you like
SUPERSET_WORKERS = 4
SUPERSET_WEBSERVER_PORT = os.environ["PORT"]

# ------------------------------------------------------------------------------
MAPBOX_API_KEY = ""

# ------------------------------------------------------------------------------
# Flask App Builder configuration
# ------------------------------------------------------------------------------
# Your App secret key
SECRET_KEY = ""

# The SQLAlchemy connection string to your database backend
# This connection defines the path to the database that stores your
# Superset metadata (slices, connections, tables, dashboards, ...).
# Note that the connection information to connect to the datasources
# you want to explore are managed directly in the web UI
SQLALCHEMY_DATABASE_URI = os.environ["DATABASE_URL"]
SQLALCHEMY_TRACK_MODIFICATIONS = True

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = CSRF_ENABLED = True

# Use all X-Forwarded headers when ENABLE_PROXY_FIX is True.
# When proxying to a different port, set "x_port" to 0 to avoid downstream issues.
ENABLE_PROXY_FIX = True

SQLLAB_ASYNC_TIME_LIMIT_SEC = int(timedelta(seconds=300).total_seconds())
SQLLAB_TIMEOUT = int(timedelta(seconds=300).total_seconds())
SUPERSET_WEBSERVER_TIMEOUT = int(timedelta(seconds=300).total_seconds())

# Grant public role the same set of permissions as for a selected builtin role.
# This is useful if one wants to enable anonymous users to view
# dashboards. Explicit grant on specific datasets is still required.
PUBLIC_ROLE_LIKE = "Gamma"
