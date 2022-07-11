# Scrapy settings for HaterCrawl project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'HaterCrawl'

PRIORITY = "link_domain_combined"
NUM_INTERVALS = 20
WAIT_INTERVAL = 3600
RESULTS_FILE = "results/hatercrawl.csv"
LOG_FILE = "logs/hatercrawl.txt"
SEED_FILE = "seeds.txt"
WORD_DICT = "word_dict.txt"
LOG_FORMAT = '[%(asctime)s] %(levelname)s: %(message)s'
MODEL_DIR = "./model"
LABEL_POS = "LABEL_1"
LABEL_NEG = "LABEL_0"
USE_SHORT_TEXT = True
SHORT_TEXT_SCORE = 0.6
THRESHOLD = 0.6

MIN_VISISTED_PAGES=25
MAX_RELEVANT_PAGES=100
WEIGHT_TEXT_SCORE = 0.7
WEIGHT_PAGE_SCORE = 0.3
WEIGHT_LINK_SURROUNDINGS = 0.5
WEIGHT_UNVISITED_DOMAINS = 0.5
WEIGHT_RELEVANT_VISITED_DOMAINS = 0.5
MIN_TEXT_SIZE = 30
MAX_TEXT_SIZE = 512
LOSS = 0.5

LANG_ACCEPTED = ['es']

SPIDER_MODULES = ['HaterCrawl.spiders']
NEWSPIDER_MODULE = 'HaterCrawl.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36"
USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7' \
    ' (KHTML, like Gecko) Chrome/16.0.912.36 Safari/535.7',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:16.0)' \
    ' Gecko/16.0 Firefox/16.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3' \
    ' (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10'
]

HTTP_PROXY = 'http://127.0.0.1:8123'
DOWNLOADER_MIDDLEWARES = {
     'HaterCrawl.middlewares.RandomUserAgentMiddleware': 400,
     'HaterCrawl.middlewares.ProxyMiddleware': 410,
     'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware': None
     # Disable compression middleware, so the actual HTML pages are cached
}


# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 3
CONCURRENT_ITEMS = 16

DNS_TIMEOUT = 15
DOWNLOAD_TIMEOUT = 30

RETRY_ENABLED = True
REDIRECT_ENABLED = True
COOKIES_ENABLED = False

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 16
CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'HaterCrawl.middlewares.HatercrawlSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'HaterCrawl.middlewares.HatercrawlDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'HaterCrawl.pipelines.ScoringPipeline': 300,
    'HaterCrawl.pipelines.StoringPipeline': 400,
    'HaterCrawl.pipelines.LinkScoringPipeline': 500,
}

MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'crawler'
MYSQL_PASSWORD = 'password'
MYSQL_DB = 'hatercrawl'
MYSQL_CHARSET = 'utf8mb4'

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
