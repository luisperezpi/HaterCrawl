from HaterCrawl.items import Text, Page, Output, Link, Link_FoundIn
from bs4 import BeautifulSoup
import scrapy
from scrapy.loader import ItemLoader
from scrapy.utils.project import get_project_settings
import re
import time
import logging
import random
import mysql.connector
import tldextract
from scrapy.exceptions import NotSupported
from langdetect import detect
import urllib.parse as urlparse

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

from stem.util.log import get_logger




#################################################################
#
#   NextURL: metodo
#       IN:     settings, diccionario con la configuración del proyecto
#       YIELD:  URL, str.
#               Lista vacía, list.
#
#       Generador de URLs de acuerdo por orden descendente de
#       relevancia. Usado para todas las prioridades salvo
#       Antbased.
#
#################################################################
def NextURL(settings):
    logging.info("Starting URL generation ")

    logging.info("Generating seed URLs ")
    f = open(settings['SEED_FILE'])
    start_urls = [url.strip() for url in f.readlines()]
    f.close()
    mydb_generator = mysql.connector.connect(
        host=settings['MYSQL_HOST'],
        user=settings['MYSQL_USER'],
        password=settings['MYSQL_PASSWORD'],
        database=settings['MYSQL_DB'],
        port=settings['MYSQL_PORT']
    )
    mycursor = mydb_generator.cursor(buffered=True)
    for url in start_urls:
        sql = 'INSERT INTO Link (link_url, score, visited, domain) ' \
              'VALUES (%s, 0.0, true, %s) ' \
              'ON DUPLICATE KEY UPDATE link_url=link_url'
        ext = tldextract.extract(url)
        mycursor.execute(sql, (url, ext.domain))
    mydb_generator.commit()
    mycursor.close()
    mydb_generator.close()

    for url in start_urls:
        logging.info("Generating seed: " + url)
        yield url, []
    logging.info("Seed file fully generated. ")

    logging.info("Generating URLs from stored data ")
    while True:
        mydb_generator = mysql.connector.connect(
            host=settings['MYSQL_HOST'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            database=settings['MYSQL_DB'],
            port=settings['MYSQL_PORT']
        )
        mycursor = mydb_generator.cursor(prepared=True)
        sql_sel = "SELECT link_url FROM Link " \
                  "WHERE visited = false " \
                  "ORDER BY score DESC LIMIT %s"
        sql_update = "UPDATE Link SET visited = true " \
                     "WHERE link_url = %s"
        try:
            mycursor.execute(sql_sel, (10,))
            res = mycursor.fetchall()
            if len(res) > 0:
                for item in res:
                    mycursor.execute(sql_update, (item[0],))
                    mydb_generator.commit()
                mycursor.close()
                mydb_generator.close()
                for item in res:
                    logging.info("Generating URL " + item[0])
                    yield item[0], []
            else:
                logging.error("The stored data has no unvisited URLs ")
                mycursor.close()
                mydb_generator.close()
                time.sleep(60)
        except Exception as e:
            logging.error("Exception found during URL generation ")
            logging.error(str(e))


#################################################################
#
#   NextURL_antbased: metodo
#       IN:     settings, diccionario con la configuración del proyecto
#       YIELD:  URL,
#               Rastro, list. Contiene el rastro de conexiones realizadas
#                           hasta la dirección devuelta
#
#       Generador de URLs de acuerdo por a lo indicado por la
#       prioridad Antbased.
#
#################################################################
def NextURL_antbased(settings):
    logging.info("Starting URL generation ")

    logging.info("Generating seed URLs ")
    f = open(settings['SEED_FILE'])
    start_urls = [url.strip() for url in f.readlines()]
    f.close()
    mydb_generator = mysql.connector.connect(
        host=settings['MYSQL_HOST'],
        user=settings['MYSQL_USER'],
        password=settings['MYSQL_PASSWORD'],
        database=settings['MYSQL_DB'],
        port=settings['MYSQL_PORT']
    )
    mycursor = mydb_generator.cursor(buffered=True)
    for url in start_urls:
        sql = 'INSERT INTO Link (link_url, score, visited, domain) ' \
              'VALUES (%s, 0.0, true, %s) ' \
              'ON DUPLICATE KEY UPDATE link_url=link_url'
        ext = tldextract.extract(url)
        mycursor.execute(sql, (url, ext.domain))
    mydb_generator.commit()
    mycursor.close()
    mydb_generator.close()

    for url in start_urls:
        logging.info("Generating seed " + url)
        yield url, []
    logging.info("Seed file fully generated. ")

    logging.info("Generating URLs from stored data ")
    sql_max_score = "SELECT MAX(Score) FROM Link_FoundIn " \
                    "WHERE page_url = %s"
    sql_sel = "SELECT DISTINCT link_url FROM Link_FoundIn " \
              "WHERE score = %s AND page_url=%s"
    sql_visited = "SELECT visited FROM Link_FoundIn " \
              "WHERE link_url=%s AND page_url=%s"
    sql_update = "UPDATE Link_FoundIn SET visited = true " \
                 "WHERE link_url=%s"
    while True:
        links_path = []
        mydb_generator = mysql.connector.connect(
            host=settings['MYSQL_HOST'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            database=settings['MYSQL_DB'],
            port=settings['MYSQL_PORT']
        )
        mycursor = mydb_generator.cursor(prepared=True)
        selected = random.choice(start_urls)
        links_path.append(selected)
        round = 0
        while True:
            # Selección de conexión aleatoria de entre aquellas con score máximo
            mycursor.execute(sql_max_score, (links_path[round],))
            res = mycursor.fetchone()
            if res[0] is None:
                break
            mycursor.execute(sql_sel, (res[0], links_path[round]))
            res = mycursor.fetchall()
            url = random.choice(res)[0]
            links_path.append(url)
            # En caso de conexión no visitada generación de URL y rastro
            mycursor.execute(sql_visited, (url, links_path[round]))
            res = mycursor.fetchall()
            if res[0][0] == 0:
                mycursor.execute(sql_update, (url, ))
                mydb_generator.commit()
                logging.info("Generating URL " + url)
                yield url, links_path
                break
            # En caso contrario se continua la busqueda
            round +=1
        mycursor.close()
        mydb_generator.close()


#################################################################
#
#   HaterCrawlSpider: clase. Implementación de scrapy.Spider
#       METHODS:
#           __init__(self, *args, **kwargs)
#           request(self): Realiza una nueva petición
#           start_requests(self): Comienza la primera petición al iniciar un rastreo
#           error(self, failure): Callback para respuestas fallidas
#           parse(self, response, link_path): Callback para respuestas correctas
#           parse_page(self, response, link_path): Método de procesado de una página
#           parse_text(self, response): Método de extracción de textos y enlaces de una página
#           get_clean_text(self, option): Método de limpieza de textos
#           get_clean_link(self, page_url, a): Método de limpieza de enlaces y creación de enlaces absolutos desde relativos
#           url_fix_whitespace(self, url): Método de sustitución de espacios en blanco en enlaces
#
#################################################################
class HaterCrawlSpider(scrapy.Spider):
    name = 'HaterCrawlSpider'
    error_count = 0
    visited_pages = set()
    settings = get_project_settings()

    #################################################################
    #
    #   __init__: metodo
    #       IN:     *args, diccionario con la configuración del proyecto
    #               **kwargs
    #
    #       Inicializador de la clase. Para esta implementación solo hace override
    #       para modificar el atributo propagate del logger. Se hace esto para evitar
    #       la alta cantidad de mensajes de la libreria controller usada en el
    #       componente Downloader Middleware: ProxyMiddleware
    #
    #################################################################
    def __init__(self, *args, **kwargs):
        super(HaterCrawlSpider, self).__init__(*args, **kwargs)
        logger = get_logger()
        logger.propagate = False

    #################################################################
    #
    #   request: metodo
    #       RETURN:     Petición formalizada, scrapy.Request.
    #
    #       Formaliza una petición con la dirección generada por el generador
    #
    #################################################################
    def request(self):
        next_url, link_path = self.url.__next__()
        if next_url is None:
            return
        else:
            return scrapy.Request(next_url, callback=self.parse, errback=self.error, cb_kwargs={'link_path': link_path}, dont_filter=True)

    #################################################################
    #
    #   start_requests: metodo
    #       YIELD:  Petición
    #
    #       Toma la información pertinente de la configuración. Esto se hace ahora
    #       pues las moficiaciones que puedan haber realizado el objeto crawler
    #       encargado de inicializar el rastreo no estan disponibles en el momento
    #       de la inicialización.
    #
    #################################################################
    def start_requests(self):
        self.concurrent = self.settings['CONCURRENT_REQUESTS']
        self.min_text = self.settings['MIN_TEXT_SIZE']
        self.max_text = self.settings['MAX_TEXT_SIZE']
        if self.settings['PRIORITY'] == "antbased":
            self.url = NextURL_antbased(self.settings)
        else:
            self.url = NextURL(self.settings)
        for i in range(0, self.concurrent):
            logging.info(i)
            yield self.request()

    #################################################################
    #
    #   error: metodo
    #
    #       Callback para procesar respuestas fallidas. Registra el tipo de
    #       respuesta y realiza una nueva petición
    #
    #################################################################
    def error(self, failure):
        self.error_count+=1
        logging.error(repr(failure))
        logging.error("Error count: " + str(self.error_count))

        if failure.check(HttpError):
            # Fallos por respuesta HTTP distinta a 200
            response = failure.value.response
            logging.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            # Fallos por timeout del DNS lookup
            request = failure.request
            logging.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            # Fallos por timeout de la descarga del recurso
            request = failure.request
            logging.error('TimeoutError on %s', request.url)
        else:
            # Otro tipo de fallos
            request = failure.request
            logging.error('UnknownError on %s', request.url)

        yield self.request()

    #################################################################
    #
    #   parse: metodo
    #       IN:     Respuesta, scrapy.Response,
    #               Rastro, list. Contiene el rastro de conexiones realizadas
    #                           hasta la dirección devuelta
    #
    #       Callback para el procesamiento de respuestas correctas
    #
    #################################################################
    def parse(self, response, link_path):
        yield self.parse_page(response, link_path)
        yield self.request()

    #################################################################
    #
    #   parse_page: metodo
    #       IN:     Respuesta , scrapy.Response
    #               Rastro, list. Contiene el rastro de conexiones realizadas
    #                           hasta la dirección devuelta
    #
    #       Metodo de procesamiento de una respuesta web. En este método se filtran
    #       las páginas por documento sin soporte, lenguaje por soporte, o
    #       recurso visitado por segunda vez.
    #       También se extrae la información relevante de la web y se llama a
    #       la función de extracción de textos y enlaces
    #
    #################################################################
    def parse_page(self, response, link_path):
        logging.info("Parsing " + response.url)
        if response.url in self.visited_pages:
            logging.error("Recurso retirado por visitada página por segunda vez " + response.url)
            return
        else:
            self.visited_pages.add(response.url)
        try:
            title_xpath = response.xpath('//title[1]/text()')
        except NotSupported:
            logging.info("Recurso retirado por tipo de documento sin soporte " + response.url)
            return
        output = Output()
        output['link_path'] = link_path
        page = ItemLoader(Page(), response)
        page.add_value('page_url', response.url)
        page.add_value('domain', response.url)
        page.add_value('deep_web', response.url)
        if len(title_xpath) == 0:
            title = response.url
        else:
            title = title_xpath.extract()[0]
        page.add_value('title', title)
        output['page'] = page.load_item()
        output['short_texts'], output['long_texts'], output['links'], output['links_foundin'] = self.parse_text(response)
        if len(output['long_texts'] + output['short_texts']) > 0:
            return output
        else:
            logging.info("Recurso retirado por lenguaje no soportado " + response.url)
            return

    #################################################################
    #
    #   parse_page: metodo
    #       IN:     Respuesta , scrapy.Response
    #
    #       Metodo de extracción de textos y enlaces de un recurso web.
    #       Se extraen de acuerdo a lo indicado en el diseño: se extraen los enlaces
    #       no descendientes de un texto, junto a su texto de anclaje si hay
    #       Y se extraen los textos no descendientes de un enlace u otro texto.
    #       Junto a los enlaces que esten contenidos dentro.
    #
    #################################################################
    def parse_text(self, response):
        links = []
        links_foundin = []
        short_texts = []
        long_texts = []
        page_url = response.url
        i = 1
        for option in response.xpath('//a'):
            if option.xpath("./parent::p|./parent::h1|./parent::h2|./parent::h3|./parent::h4|./parent::span|./parent::a"):
                continue
            if not option.xpath("@href"):
                continue
            link_url = self.get_clean_link(page_url, option)
            if link_url is None:
                continue
            link = ItemLoader(Link(), option)
            link.add_value('link_url', link_url)
            link.add_value('domain', link_url)
            links.append(link.load_item())
            clean_texts = self.get_clean_text(option)
            found = 0
            for clean_text, type_text in clean_texts:
                text = ItemLoader(Text(), option)
                try:
                    if detect(clean_text) not in self.settings['LANG_ACCEPTED']:
                        continue
                except Exception as e:
                    continue
                text.add_value('text', clean_text)
                text.add_value('page_url', page_url)
                text.add_value('id_in_page', i)
                if type_text == "long":
                    long_texts.append(text.load_item())
                elif type_text == "short":
                    if self.settings['USE_SHORT_TEXT'] == True:
                        short_texts.append(text.load_item())
                    else:
                        continue
                else:
                    logging.error("Leido texto de forma incorrecta " + text.load_item())
                    continue
                found = 1
                link_foundin = ItemLoader(Link_FoundIn(), option)
                link_foundin.add_value('page_url', page_url)
                link_foundin.add_value('link_url', link_url)
                link_foundin.add_value('id_in_page', i)
                links_foundin.append(link_foundin.load_item())
                i += 1
            if found == 0:
                link_foundin = ItemLoader(Link_FoundIn(), option)
                link_foundin.add_value('page_url', page_url)
                link_foundin.add_value('link_url', link_url)
                link_foundin.add_value('id_in_page', 0)
                links_foundin.append(link_foundin.load_item())

        for option in response.xpath('//p | //h1 | //h2 | //h3 | //h4 | //span'):
            if option.xpath("./parent::p|./parent::h1|./parent::h2|./parent::h3|./parent::h4|./parent::span|./parent::a"):
                continue
            clean_texts = self.get_clean_text(option)
            for clean_text, type_text in clean_texts:
                text = ItemLoader(Text(), option)
                text.add_value('text', clean_text)
                text.add_value('page_url', page_url)
                text.add_value('id_in_page', i)
                if type_text == "long":
                    long_texts.append(text.load_item())
                elif type_text == "short":
                    if self.settings['USE_SHORT_TEXT'] == True:
                        short_texts.append(text.load_item())
                    else:
                        continue
                else:
                    logging.error("Leido texto de forma incorrecta " + text.load_item())
                    continue
                for item in option.xpath('.//a'):
                    if not option.xpath("@href"):
                        continue
                    link_url = self.get_clean_link(page_url, option)
                    if link_url is None:
                        continue

                    link = ItemLoader(Link(), item)
                    link.add_value('link_url', link_url)
                    link.add_value('domain', link_url)
                    links.append(link.load_item())
                    link_foundin = ItemLoader(Link_FoundIn(), option)
                    link_foundin.add_value('page_url', page_url)
                    link_foundin.add_value('link_url', link_url)
                    link_foundin.add_value('id_in_page', i)
                    links_foundin.append(link_foundin.load_item())
                i += 1

        return short_texts, long_texts, links, links_foundin

    #################################################################
    #
    #   get_clean_text: metodo
    #       IN:     selector, Selector de XPATH con el nodo texto a limpiar
    #
    #       Metodo de procesamiento de un texto. Devuelve una lista de textos,
    #       junto a su tipo. Dividiendo en textos menores si el nodo a
    #       procesar es demasiado largo
    #
    #################################################################
    def get_clean_text(self, selector):
        clean_texts = []
        soup = BeautifulSoup(selector.extract(), 'html.parser')
        text = soup.get_text()
        text = text.strip('\n').strip('\t').rstrip().lstrip()

        if len(text) >= self.max_text:
            temp_text = ""
            for iter_text in text.split("."):
                if len(iter_text) >= self.max_text-1:
                    continue
                elif len(temp_text) + len(iter_text) <= self.max_text-1:
                    temp_text = temp_text + iter_text + "."
                    if len(temp_text) > self.min_text:
                        clean_texts.append((temp_text, "long"))
                    elif 0 < len(temp_text) <= self.min_text:
                        clean_texts.append((temp_text, "short"))
                    temp_text = ""
                else:
                    temp_text = temp_text + iter_text + "."
        elif len(text) > self.min_text:
            clean_texts.append((text, "long"))
        elif 0 < len(text) <= self.min_text:
            clean_texts.append((text, "short"))
        return clean_texts

    #################################################################
    #
    #   get_clean_link: metodo
    #       IN:     page_url, url de la página donde se encontro el enlace
    #               a, Selector de XPATH con el nodo enlace a limpiar
    #
    #       Metodo de procesamiento de un enlace. Extrae la dirección de un nodo y construye
    #       la dirección absoluta en caso de que sea relativa.
    #
    #################################################################
    def get_clean_link(self, page_url, a):
        str = a.xpath("@href").extract()[0]
        if re.match("http.+", str):
            str = str
        elif re.match("/.*", str):
            domain = re.findall("http.*//[^/]+", page_url)[0]
            str = domain + str
        else:
            return None
        str = self.url_fix_whitespace(str)
        if len(str) >= 512:
            return None
        return str

    #################################################################
    #
    #   url_fix_whitespace: metodo
    #       IN:     url, url a limpiar
    #
    #       Metodo de limpieza de una dirección URL.
    #
    #################################################################
    def url_fix_whitespace(self, url):
        scheme, netloc, path, qs, anchor = urlparse.urlsplit(url)
        path = urlparse.quote(path, '/%')
        qs = urlparse.quote_plus(qs, ':&=')
        return urlparse.urlunsplit((scheme, netloc, path, qs, anchor))

