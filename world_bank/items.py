# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class WorldBankDataItem(scrapy.Item):
    name = scrapy.Field()
    url = scrapy.Field()
    filetype = scrapy.Field()
    status = scrapy.Field()
    filepath = scrapy.Field()
    scrape_time = scrapy.Field()
    public_time = scrapy.Field()