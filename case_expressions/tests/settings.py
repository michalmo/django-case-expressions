SECRET_KEY = "django_tests_secret_key"

DEBUG = False
TEMPLATE_DEBUG = False

ALLOWED_HOSTS = []

MIDDLEWARE_CLASSES = ()
INSTALLED_APPS = (
    'case_expressions.tests',
)
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
    }
}
