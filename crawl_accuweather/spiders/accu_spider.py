from datetime import datetime, timedelta

import scrapy
from scrapy.crawler import CrawlerProcess

from crawl_accuweather.dao.DBAppender import DBAppender


class AccuweatherSpider(scrapy.Spider):
    name = "accu_spider"
    TABLE_FIELD_TIME = 'time'
    TABLE_FIELD_HIGH_TEMP = 'htemp'
    TABLE_FIELD_LOW_TEMP = 'ltemp'
    TABLE_FIELD_CRAWLED_TIME = 'crawled_time'

    def __init__(self, url_pattern=None, country_code='vn',
                 loc_name='ho-chi-minh-city', loc_code='353981',
                 from_dt=None, to_dt=None,
                 table_name='tphcm_weather_day_temp_accuweather_crawled',
                 table_fields=('time', 'htemp', 'ltemp', 'crawled_time'),
                 parse_func=None, crawled_dt_format='%Y-%m-%d %H:00:00:00',
                 **kwargs):
        super(AccuweatherSpider, self).__init__(**kwargs)
        self.url_pattern = url_pattern or 'https://www.accuweather.com/en/' \
                                          '{country_code}/{loc_name}/{loc_code}/{month_name}-weather/{loc_code}' \
                                          '?monyr={month}/1/{year}&view=table'
        self.country_code = country_code
        self.loc_name = loc_name
        self.loc_code = loc_code
        self.table_name = table_name
        self.table_fields = table_fields
        self.from_dt = from_dt
        self.to_dt = to_dt
        self.parse_func = parse_func
        self.crawled_dt_format = crawled_dt_format
        self.crawled_urls = set()

    def start_requests(self):
        from_dt = self.from_dt or datetime.now()
        to_dt = self.to_dt or from_dt + timedelta(days=90)
        dt = from_dt
        while dt <= to_dt:
            url = self.format_url(dt)
            if url not in self.crawled_urls:
                self.crawled_urls.add(url)
                yield scrapy.Request(url=url, callback=self.parse)
            dt += timedelta(days=1)

    def format_url(self, dt=datetime.now()):
        month_name = dt.strftime('%B').lower()
        month = dt.month
        year = dt.year
        url = self.url_pattern.format(**{'country_code': self.country_code,
                                         'loc_name': self.loc_name, 'loc_code': self.loc_code,
                                         'month_name': month_name, 'month': month, 'year':year})
        return url

    def parse(self, response):
        if self.parse_func is not None:
            self.parse_func(response)
        else:
            year = response.css('.btr-increment > span:nth-child(1)::text').extract()[0].encode('ascii', 'ignore')
            row = []
            pre_dts = response.css('tr.pre th:nth-child(1) time:nth-child(1)::text').extract()
            pre_lhs = response.css('tr.pre > td:nth-child(2)::text').extract()
            row.extend(zip(pre_dts, pre_lhs))

            post_dts = response.css('tr.lo > th:nth-child(1) > a:nth-child(1) > time:nth-child(1)::text').extract()
            post_lhs = response.css('tr.calendar-list-cl-tr td:nth-child(2)::text').extract()
            row.extend(zip(post_dts, post_lhs))

            data = []

            crawled_time = datetime.strptime(datetime.now().strftime(self.crawled_dt_format), self.crawled_dt_format)
            for raw_dt, raw_hls in row:
                try:
                    month_date = raw_dt.encode('ascii', 'ignore')
                    dt = datetime.strptime('%s/%s' % (year, month_date), '%Y/%m/%d')
                    hls = raw_hls.encode('ascii', 'ignore').split('/')
                    htemp = int(hls[0])
                    ltemp = int(hls[1])
                    if (self.from_dt is None or self.from_dt.toordinal() <= dt.toordinal()) \
                            and (self.to_dt is None or dt.toordinal() <= self.to_dt.toordinal()):
                        datum_dict = {self.TABLE_FIELD_TIME: dt,
                                      self.TABLE_FIELD_HIGH_TEMP: htemp,
                                      self.TABLE_FIELD_LOW_TEMP: ltemp,
                                      self.TABLE_FIELD_CRAWLED_TIME: crawled_time}
                        record = tuple([datum_dict.get(key) for key in self.table_fields])
                        data.append(record)

                except Exception, e:
                    pass

            appender = DBAppender(table_name=self.table_name, fields=self.table_fields)
            appender.insert(data)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(AccuweatherSpider())
    process.start()
