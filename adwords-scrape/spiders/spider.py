import scrapy
import re
import logging
from adwords_scrape.spiders.database.db import DbConnection

class KeywordsSpider(scrapy.Spider):
    name = "keywords"

    def __init__(self):
        db = DbConnection()
        self.keywords = db.import_keywords()
        super().__init__(self)

    def start_requests(self):
        """Scrape starts here: perform a Google search for each keyword."""
        for row in self.keywords:
            yield scrapy.Request(
                url="https://www.google.com/search?q=" + row['keyword'],
                callback=self.parse_google_results,
                meta={
                    'dont_redirect': True,
                    'keyword_id': row['id']
                }
            )

    def parse_google_results(self, response):
        """For each Google search result, navigate to it's URL."""
        rank = 0

        for url in response.xpath('//h3[@class="r"]/a/@href').extract():
            rank += 1
            # Remove Google's added string from the result's URL
            url = url.strip('/url?q=')
            url = re.sub(r'&sa=.+', '', url)

            yield scrapy.Request(
                url=url,
                callback=self.parse_google_result,
                meta={
                    'keyword_id': response.meta['keyword_id'],
                    'rank': rank
                }
            )

    def parse_google_result(self, response):
        """Return data from the page."""
        yield {
            'keyword_id':       response.meta['keyword_id'],
            'rank':             response.meta['rank'],
            'url':              response.request.url,
            'meta':             response.xpath('//meta').extract(),
            'h1':               response.xpath('//h1/text()').extract(),
            'h2':               response.xpath('//h2/text()').extract(),
            'p':                response.xpath('//p/text()').extract()[0],
            'text_content':     ''.join(response.xpath('//body//text()').extract()).strip(),
            'a_external':       self.get_external_links(response, response.request.url),
            'a_internal':       self.get_internal_links(response, response.request.url),
            'img':              response.xpath('//img/@src').extract(),
            'img_alt':          response.xpath('//img/@alt').extract()
        }

    def get_links(self, response):
        """Return all links."""
        return response.xpath('//a/@href').extract()

    def get_external_links(self, response, url):
        """Return links that point to outside the site."""
        links = self.get_links(response)
        return list(filter(lambda link: not self.is_link_internal(url, link), links))

    def get_internal_links(self, response, url):
        """Return links that point to inside the site."""
        links = self.get_links(response)
        return list(filter(lambda link: self.is_link_internal(url, link), links))

    def is_link_internal(self, url, link):
        """Returns True if link points to somewhere within the current site."""
        return url not in link or re.fullmatch(r'^(\/)([^\/]).+', link)
