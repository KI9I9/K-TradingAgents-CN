"""在仓库根目录生成全新环境配置，绝不覆盖现有密钥。"""
from pathlib import Path
import os
import secrets

root = Path(__file__).resolve().parent.parent
target = root / '.env'
if target.exists():
    raise SystemExit('.env 已存在，为避免改变数据库密码，本次未覆盖。')
mongo = secrets.token_hex(24)
redis = secrets.token_hex(24)
admin = secrets.token_urlsafe(18)
values = {
    'WEB_PORT': '8088',
    'DEBUG': 'false',
    'HOST': '0.0.0.0',
    'PORT': '8000',
    'TIMEZONE': 'Asia/Shanghai',
    'TZ': 'Asia/Shanghai',
    'MONGODB_ENABLED': 'true',
    'MONGODB_HOST': 'mongodb',
    'MONGODB_PORT': '27017',
    'MONGODB_USERNAME': 'admin',
    'MONGODB_PASSWORD': mongo,
    'MONGODB_DATABASE': 'k_tradingagents',
    'MONGODB_DATABASE_NAME': 'k_tradingagents',
    'MONGODB_AUTH_SOURCE': 'admin',
    'MONGODB_CONNECTION_STRING': f'mongodb://admin:{mongo}@mongodb:27017/k_tradingagents?authSource=admin',
    'REDIS_ENABLED': 'true',
    'REDIS_HOST': 'redis',
    'REDIS_PORT': '6379',
    'REDIS_PASSWORD': redis,
    'REDIS_DB': '0',
    'JWT_SECRET': secrets.token_hex(48),
    'CSRF_SECRET': secrets.token_hex(48),
    'ADMIN_DEFAULT_PASSWORD': admin,
    'ALLOWED_ORIGINS': '["http://localhost:8088","http://127.0.0.1:8088"]',
    'ALLOWED_HOSTS': '["localhost","127.0.0.1","backend"]',
    'LOG_LEVEL': 'INFO',
    'TRADINGAGENTS_LOG_LEVEL': 'INFO',
    'DEFAULT_CHINA_DATA_SOURCE': 'akshare',
    'VECTOR_STORE_BACKEND': 'mongodb',
    'MEM0_ENABLED': 'false',
    'MEMORY_ENABLED': 'false',
    'TA_USE_APP_CACHE': 'true',
    'USE_MONGODB_STORAGE': 'true',
    'JDYUN_MODE': 'false',
    'LICENSE_SKIP_PRO_CHECK': 'false',
    'DEFAULT_USER_CONCURRENT_LIMIT': '1',
    'GLOBAL_CONCURRENT_LIMIT': '3',
    'DEFAULT_DAILY_QUOTA': '50',
    'DEEPSEEK_API_KEY': '',
    'DASHSCOPE_API_KEY': '',
    'OPENAI_API_KEY': '',
    'TUSHARE_TOKEN': '',
    'TUSHARE_ENABLED': 'false',
    'TUSHARE_UNIFIED_ENABLED': 'false',
    'AKSHARE_UNIFIED_ENABLED': 'true',
    'AKSHARE_BASIC_INFO_SYNC_ENABLED': 'false',
    'AKSHARE_QUOTES_SYNC_ENABLED': 'false',
    'AKSHARE_HISTORICAL_SYNC_ENABLED': 'false',
    'AKSHARE_FINANCIAL_SYNC_ENABLED': 'false',
    'AKSHARE_STATUS_CHECK_ENABLED': 'false',
    'BAOSTOCK_UNIFIED_ENABLED': 'false',
    'QUOTES_INGEST_ENABLED': 'false',
    'QUOTES_BACKFILL_ON_STARTUP': 'false',
    'QUOTES_BACKFILL_ON_OFFHOURS': 'false',
    'SYNC_STOCK_BASICS_ENABLED': 'false',
    'ETF_SYNC_ENABLED': 'false',
    'FAVORITES_DATA_SYNC_ENABLED': 'false',
    'NEWS_SYNC_ENABLED': 'false',
    'WATCHLIST_ANALYSIS_ENABLED': 'false',
}
os.umask(0o077)
with target.open('x', encoding='utf-8') as file:
    file.write('# 初次安装：先配置模型和数据源，再启用所需的数据同步与记忆功能。\n')
    file.writelines(f'{key}={value}\n' for key, value in values.items())
runtime = root / 'runtime'
runtime.mkdir(exist_ok=True)
login = runtime / 'initial-login.txt'
login.write_text(f'访问地址：http://127.0.0.1:8088\n账号：admin\n初始密码：{admin}\n请登录后修改密码。\n', encoding='utf-8')
login.chmod(0o600)
print('已生成权限为 600 的 .env 和 runtime/initial-login.txt；密钥未输出。')
