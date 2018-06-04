from datetime import datetime, timedelta

from scrapy.crawler import CrawlerProcess

from crawl_accuweather.spiders.accu_spider import AccuweatherSpider

url_pattern = 'https://www.accuweather.com/en/' \
              '{country_code}/{loc_name}/{loc_code}/{month_name}-weather/{loc_code}' \
              '?monyr={month}/1/{year}&view=table'
country_code = 'vn'
loc_name = 'ho-chi-minh-city'
loc_code = '353981'
table_name = 'tphcm_weather_day_temp_accuweather_crawled_forecast'
table_fields = ('time', 'htemp', 'ltemp', 'crawled_time')
# from_dt = datetime.now() + timedelta(days=-10)
from_dt = datetime.now()  # now if not specified
# from_dt = None  # now if not specified
# to_dt = from_dt + timedelta(days=90)
to_dt = None  # from_dt + 90 days if not specified
process = CrawlerProcess()
process.crawl(AccuweatherSpider,
              url_pattern=url_pattern,
              country_code=country_code,
              loc_name=loc_name,
              loc_code=loc_code,
              from_dt=from_dt,
              to_dt=to_dt,
              table_name=table_name,
              table_fields=table_fields)
process.start()
