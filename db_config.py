"""Database configuration shared by the API and desktop server runner."""

import os
from sqlalchemy.engine import URL, make_url


def database_url(environ=None):
    """Build a safe SQLAlchemy URL; raw component passwords need no URL encoding.

    Explicit URLs take precedence. MYSQL_NAME is the database name; MYSQL_USER
    is the login. Lowercase spellings are accepted for .env convenience.
    Return None only when no database settings have been supplied.
    """
    env = {key.upper(): value for key, value in (os.environ if environ is None else environ).items()}
    for key in ('DATABASE_URL', 'MYSQL_PUBLIC_URL', 'MYSQL_URL', 'RAILWAY_SERVICE_MYSQL_URL'):
        value = env.get(key, '').strip()
        if value:
            if '://' not in value:
                value = 'mysql+pymysql://' + value
            url = make_url(value)
            if url.drivername == 'mysql':
                url = url.set(drivername='mysql+pymysql')
            return url.render_as_string(hide_password=False)

    def setting(*keys, default=''):
        return next((env[key] for key in keys if env.get(key, '') != ''), default)

    host = setting('MYSQL_HOST', 'RAILWAY_MYSQL_HOST').strip()
    user = setting('MYSQL_USER', 'RAILWAY_MYSQL_USER').strip()
    name = setting('MYSQL_NAME', 'MYSQL_DATABASE', 'RAILWAY_MYSQL_DATABASE').strip()
    password = setting('MYSQL_PASSWORD', 'RAILWAY_MYSQL_PASSWORD')
    port = setting('MYSQL_PORT', 'RAILWAY_MYSQL_PORT', default='3306').strip()
    supplied = any(env.get(key, '') for key in (
        'MYSQL_HOST', 'MYSQL_USER', 'MYSQL_NAME', 'MYSQL_DATABASE', 'MYSQL_PASSWORD',
        'MYSQL_PORT', 'RAILWAY_MYSQL_HOST', 'RAILWAY_MYSQL_USER',
        'RAILWAY_MYSQL_DATABASE', 'RAILWAY_MYSQL_PASSWORD', 'RAILWAY_MYSQL_PORT',
    ))
    if not supplied:
        return None
    if not host or not user or not name:
        raise ValueError('Set MYSQL_HOST, MYSQL_USER and MYSQL_NAME together (or remove all MySQL settings for SQLite).')
    try:
        port_number = int(port)
    except ValueError:
        raise ValueError('MYSQL_PORT must be a number between 1 and 65535.') from None
    if not 1 <= port_number <= 65535:
        raise ValueError('MYSQL_PORT must be a number between 1 and 65535.')
    return URL.create(
        'mysql+pymysql', username=user, password=password, host=host,
        port=port_number, database=name,
    ).render_as_string(hide_password=False)
