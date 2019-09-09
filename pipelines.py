# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from adwords_scrape.spiders.database.db import DbConnection


class AdwordsScrapePipeline(object):
    def process_item(self, item, spider):
        return item


class MySQLPipeline(object):
    def __init__(self):
        self.db = DbConnection()

    def process_item(self, item, spider):
        self.db.insert_result(item)
        return item
