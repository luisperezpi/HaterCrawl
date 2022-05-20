# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import logging
import os
import random
from scrapy import signals
from stem import Signal
from stem.control import Controller
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


#################################################################
#
#   RandomUserAgentMiddleware: ProxyMiddleware. Implementación de una clase UserAgentMiddleware
#       METHODS:
#           __init__: instanciador. User-Agents de la configuración
#           process_request(self, request, spider):
#                   Método que procesa un objeto Request para modificar
#                   el atributo User-Agent de la cabecera
#
#################################################################
class RandomUserAgentMiddleware(UserAgentMiddleware):

    def __init__(self, settings):
        super(RandomUserAgentMiddleware, self).__init__()
        user_agent_list = settings['USER_AGENT_LIST']
        if not user_agent_list:
            ua = settings['USER_AGENT']
            self.user_agent_list = [ua]
        else:
            self.user_agent_list = user_agent_list

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls(crawler.settings)
        return obj

    #################################################################
    #
    #   process_request: metodo
    #       IN:     request, scrapy.Request
    #               spider, scrapy.Spider
    #
    #       Procesa cada request generado por una instancia de scrapy.Spider
    #
    #################################################################
    def process_request(self, request, spider):
        user_agent = random.choice(self.user_agent_list)
        if user_agent:
            request.headers.setdefault('User-Agent', user_agent)
        else:
            logging.error("Error al realizar el cambio de User-Agent")



#################################################################
#
#   new_tor_identity: metodo
#
#       Mediante comunicación con el controller de TOR pide
#       un cambio de identidad
#
#################################################################
def new_tor_identity():
    with Controller.from_port(port=9051) as controller:
        controller.authenticate(password='hater_password')
        controller.signal(Signal.NEWNYM)


#################################################################
#
#   ScoringPipeline: ProxyMiddleware. Implementación de una clase HttpProxyMiddleware
#       METHODS:
#           process_request(self, request, spider):
#                   Método que procesa un objeto Request para modificar el
#                   proxy y cambiar la identidad de tor
#
#################################################################
class ProxyMiddleware(HttpProxyMiddleware):

    #################################################################
    #
    #   process_request: metodo
    #       IN:     request, scrapy.Request
    #               spider, scrapy.Spider
    #
    #       Procesa cada request generado por una instancia de scrapy.Spider
    #
    #################################################################
    def process_request(self, request, spider):
        logging.info("Changing proxy for " + request.url)
        # Set the Proxy
        # A new identity for each request
        # Comment out if you want to get a new Identity only through process_response
        new_tor_identity()
        request.meta['proxy'] = 'http://127.0.0.1:8118'


class HatercrawlSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class HatercrawlDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
