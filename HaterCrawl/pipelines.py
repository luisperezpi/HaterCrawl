# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from transformers import pipeline
from transformers import BertForSequenceClassification, BertTokenizer
from transformers.pipelines.pt_utils import KeyDataset
from datetime import datetime
import logging
import re
import time
import pymysql


#################################################################
#
#   ScoringPipeline: clase. Implementación de un Item Pipeline de la
#                           arquitectura Scrapy, de forma similar a
#                           scrapy.ItemPipeline
#       METHODS:
#           from_crawler(cls, crawler): @classsmethod que el crawler ejecuta antes de
#                           instanciar esta clase
#           __init__(self, crawler): Instanciador
#           process_item(self, item, spider): Metodo que procesa cada objeto
#                                               producidos por los elementos scrapy.Spider
#           _scoring(self, item): Método encargado de la valoración de textos
#
#################################################################
class ScoringPipeline:

    #################################################################
    #
    #   from_crawler: @classmethod
    #       IN:     crawler, scrapy.crawler.
    #
    #       Es usado por el crawler antes de instanciar esta clase.
    #       Necesario para enviar como argumento el crawler al
    #       instanciador dado que si se toma las settings de otra
    #       forma puede que no se hayan cambiado
    #
    #################################################################
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    #################################################################
    #
    #   __init__: metodo instanciador
    #       IN:     crawler, scrapy.crawler.
    #
    #       Instancia los elementos necesarios de acuerdo a la configuración.
    #       Debe instaurar el pipeline para la clasificación de textos largos,
    #       y el keyset para la clasificación de textos cortos.
    #
    #################################################################
    def __init__(self, crawler):
        self.settings = crawler.settings
        str_path = self.settings['MODEL_DIR']
        self.model = BertForSequenceClassification.from_pretrained(str_path)
        self.tokenizer = BertTokenizer.from_pretrained(str_path)
        self.pipe = pipeline("text-classification", model=self.model, tokenizer=self.tokenizer)
        self.word_set = set()
        self.label_pos = self.settings['LABEL_POS']
        self.label_neg = self.settings['LABEL_NEG']
        with open(self.settings['WORD_DICT']) as file:
            for word in file.readlines():
                self.word_set.add(str.lower(word).strip("\n"))

    #################################################################
    #
    #   process_item: metodo
    #       IN:     item, scrapy.Item
    #               spider, scrapy.Spider
    #
    #       Procesa cada objeto generado por una instancia de scrapy.Spider
    #
    #################################################################
    def process_item(self, item, spider):
        logging.info("Scoring Texts for " + item['page']['page_url'])
        return self._scoring(item)

    #################################################################
    #
    #   _scoring: metodo
    #       IN:     item, scrapy.Item
    #
    #       Valora los textos largos de acuerdo a un modelo y los
    #       textos cortos de acuerdo a un dataset. A continuación
    #       valora las páginas.
    #
    #################################################################
    def _scoring(self, item):
        score=0.0
        found_hate_texts=0
        i=0
        for text in self.pipe(KeyDataset(item['long_texts'], 'text')):
            if text['label'] == self.label_pos:
                score += text['score']
                found_hate_texts+=1
                item['long_texts'][i]['score']= (text['score'] + 1)/2
                item['long_texts'][i]['label']= text['label']
            else:
                item['long_texts'][i]['score']= (1 - text['score'])/2
                item['long_texts'][i]['label']= text['label']
            i+=1

        if self.settings['USE_SHORT_TEXT'] == True:
            i=0
            for text in item['short_texts']:
                item['short_texts'][i]['score'] = 0.0
                item['short_texts'][i]['label'] = self.label_neg
                for word in re.split(r'\W+', text['text']):
                    if str.lower(word) in self.word_set:
                        item['short_texts'][i]['score'] = self.settings['SHORT_TEXT_SCORE']
                        item['short_texts'][i]['label'] = self.label_pos
                        score += self.settings['SHORT_TEXT_SCORE']
                        found_hate_texts+=1
                        break
                i+=1

        multiplier = -1/(found_hate_texts+0.5)+1
        if found_hate_texts > 0:
            item['page']['score'] = score/found_hate_texts * multiplier
            item['page']['found_hate_texts'] = found_hate_texts
        else:
            item['page']['score'] = 0
            item['page']['found_hate_texts'] = 0
        return item


#################################################################
#
#   StoringPipeline: clase. Implementación de un Item Pipeline de la
#                           arquitectura Scrapy, de forma similar a
#                           scrapy.ItemPipeline
#       METHODS:
#           from_crawler(cls, crawler): @classsmethod que el crawler ejecuta antes de
#                           instanciar esta clase
#           __init__(self, crawler): Instanciador
#           process_item(self, item, spider): Metodo que procesa cada objeto
#                                               producidos por los elementos scrapy.Spider
#           _retry_sql(self, tx, sql, tuple): Método encargado de repetir un cierto número
#                                               de veces una petición sql
#           _insert_page(self, tx, item): Método encargado de almacenar un
#                                           objeto Page
#           _insert_text(self, tx, item): Método encargado de almacenar un
#                                           objeto Text
#           _insert_link(self, tx, item): Método encargado de almacenar un
#                                           objeto Link
#
#################################################################
class StoringPipeline:

    #################################################################
    #
    #   from_crawler: @classmethod
    #       IN:     crawler, scrapy.crawler.
    #
    #       Es usado por el crawler antes de instanciar esta clase.
    #       Necesario para enviar como argumento el crawler al
    #       instanciador dado que si se toma las settings de otra
    #       forma puede que no se hayan cambiado
    #
    #################################################################
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    #################################################################
    #
    #   __init__: metodo instanciador
    #       IN:     crawler, scrapy.crawler.
    #
    #       Instancia los elementos necesarios de acuerdo a la configuración.
    #       Debe guardar la configuración
    #
    #################################################################
    def __init__(self, crawler):
        self.settings = crawler.settings


    #################################################################
    #
    #   process_item: metodo
    #       IN:     item, scrapy.Item
    #               spider, scrapy.Spider
    #
    #       Procesa cada objeto generado por una instancia de scrapy.Spider.
    #       En particular, toma toda la información de un objeto Output y
    #       almacena cada uno de sus componentes.
    #
    #################################################################
    def process_item(self, item, spider):
        self.mydb = pymysql.connect(host=self.settings['MYSQL_HOST'],
                                     user=self.settings['MYSQL_USER'],
                                     password=self.settings['MYSQL_PASSWORD'],
                                     database=self.settings['MYSQL_DB'],
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        page = item['page']
        url = page['page_url']
        texts = item['short_texts'] + item['long_texts']
        links = item['links']
        links_foundin = item['links_foundin']

        try:
            self.mydb.commit()
            tx = self.mydb.cursor()
            logging.info("Storing Page for " + url)
            self._insert_page(tx, page)
            tx.close()
            self.mydb.commit()

            tx = self.mydb.cursor()
            logging.info("Storing Texts for " + url)
            for text in texts:
                self._insert_text(tx, text)
            tx.close()
            self.mydb.commit()

            tx = self.mydb.cursor()
            logging.info("Storing Links for " + url)
            for link in links:
                self._insert_link(tx, link)
            for link_foundin in links_foundin:
                self._insert_link_foundin(tx, link_foundin)
            tx.close()
            self.mydb.commit()
        except Exception as e:
            return item
        self.mydb.close()
        return item

    #################################################################
    #
    #   _retry_sql: metodo
    #       IN:     tx, cursor de pymysql
    #               sql, peticion sql a realizar
    #               tuple, argumentos con los que realizar una petición
    #
    #       Repite una petición sql en un cursor hasta 3 veces.
    #
    #################################################################
    def _retry_sql(self, tx, sql, tuple):
        retries = 1
        while True:
            try:
                tx.execute(sql, tuple)
                return
            except Exception as e:
                if retries > 3:
                    raise e
                    return
                else:
                    if retries == 0:
                        print(str(e))
                    retries+=1
                    time.sleep(0.1)
                    pass


    #################################################################
    #
    #   _insert_page: metodo
    #       IN:     tx, cursor de pymysql
    #               item, Page. Representa la página a almacenar
    #
    #       Almacena la información de un objeto Page
    #
    #################################################################
    def _insert_page(self, tx, item):
        if item['score'] > self.settings['THRESHOLD']:
            sql_template_1 = 'INSERT INTO Domain (domain, found_hate_pages, found_pages, discovered_time, last_seen_time) VALUES (%s, 1, 1, %s, %s) ' \
                           'ON DUPLICATE KEY UPDATE domain=%s, found_hate_pages=found_hate_pages+1, found_pages=found_pages+1, last_seen_time=%s'
        else:
            sql_template_1 = 'INSERT INTO Domain (domain, found_hate_pages, found_pages, discovered_time, last_seen_time) VALUES (%s, 0, 1, %s, %s) ' \
                           'ON DUPLICATE KEY UPDATE domain=%s, found_pages=found_pages+1, last_seen_time=%s'

        sql_template_2 = 'INSERT INTO Page (page_url, title, score, found_hate_texts, domain, time, deep_web) ' \
                       'VALUES (%s, %s, %s, %s, %s, %s, %s) '

        sql_template_3 = 'INSERT INTO Text (id_in_page, page_url, label, score) ' \
                         'VALUES (0, %s, %s, 0.0) '

        formatted_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            self._retry_sql(tx, sql_template_1, (item['domain'], formatted_date, formatted_date, item['domain'], formatted_date))
            self._retry_sql(tx, sql_template_2,
                       (item['page_url'], item['title'], item['score'], item['found_hate_texts'], item['domain'], formatted_date, item['deep_web']))
            self._retry_sql(tx, sql_template_3,
                            (item['page_url'], self.settings['LABEL_NEG']))
        except Exception as e:
            print(str(e))
            print(item)
            raise e

    #################################################################
    #
    #   _insert_text: metodo
    #       IN:     tx, cursor de pymysql
    #               item, Text. Representa la página a almacenar
    #
    #       Almacena la información de un objeto Text
    #
    #################################################################
    def _insert_text(self, tx, item):
        sql_template = 'INSERT INTO Text (id_in_page, text, page_url, label, score) ' \
                       'VALUES (%s, %s, %s, %s, %s) '
        try:
            self._retry_sql(tx,sql_template,
                       (item['id_in_page'], item['text'], item['page_url'],
                        item['label'], item['score']))
        except Exception as e:
            print(str(e))
            print(item)
            raise e

    #################################################################
    #
    #   _retry_sql: metodo
    #       IN:     tx, cursor de pymysql
    #               item, Link. Representa la página a almacenar
    #
    #       Almacena la información de un objeto Link
    #
    #################################################################
    def _insert_link(self, tx, item):

        sql_template_1 = 'INSERT INTO Link (link_url, score, visited, domain) ' \
                       'VALUES (%s, 0.0, false, %s) ' \
                       'ON DUPLICATE KEY UPDATE link_url=link_url'
        sql_template_2 = 'INSERT INTO Link_FoundIn (id_in_page, page_url, link_url, score, visited) ' \
                       'VALUES (%s, %s, %s, 1, false) '
        try:
            self._retry_sql(tx,sql_template_1,
                       (item['link_url'], item['domain']))
        except Exception as e:
            print(str(e))
            print(item)
            raise e


    #################################################################
    #
    #   _retry_sql: metodo
    #       IN:     tx, cursor de pymysql
    #               item, Link. Representa la página a almacenar
    #
    #       Almacena la información de un objeto Link
    #
    #################################################################
    def _insert_link_foundin(self, tx, item):

        sql_template_1 = 'INSERT INTO Link (link_url, score, visited, domain) ' \
                       'VALUES (%s, 0.0, false, %s) ' \
                       'ON DUPLICATE KEY UPDATE link_url=link_url'
        sql_template_2 = 'INSERT INTO Link_FoundIn (id_in_page, page_url, link_url, score, visited) ' \
                       'VALUES (%s, %s, %s, 1, false) '
        try:
            self._retry_sql(tx,sql_template_2,
                       (item['id_in_page'], item['page_url'], item['link_url']))
        except Exception as e:
            print(str(e))
            print(item)
            raise e

#################################################################
#
#   LinkScoringPipeline: clase. Implementación de un Item Pipeline de la
#                           arquitectura Scrapy, de forma similar a
#                           scrapy.ItemPipeline
#       METHODS:
#           from_crawler(cls, crawler): @classsmethod que el crawler ejecuta antes de
#                           instanciar esta clase
#           __init__(self, crawler): Instanciador
#           process_item(self, item, spider): Metodo que procesa cada objeto
#                                               producidos por los elementos scrapy.Spider
#           _scoring(self, item): Método encargado de la valoración de textos
#
#################################################################
class LinkScoringPipeline:

    #################################################################
    #
    #   from_crawler: @classmethod
    #       IN:     crawler, scrapy.crawler.
    #
    #       Es usado por el crawler antes de instanciar esta clase.
    #       Necesario para enviar como argumento el crawler al
    #       instanciador dado que si se toma las settings de otra
    #       forma puede que no se hayan cambiado
    #
    #################################################################
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    #################################################################
    #
    #   __init__: metodo instanciador
    #       IN:     crawler, scrapy.crawler.
    #
    #       Instancia los elementos necesarios de acuerdo a la configuración.
    #       Debe guardar la configuración y elegir el método a realizar para
    #       la valoración
    #
    #################################################################
    def __init__(self, crawler):
        self.settings = crawler.settings
        self.wts = self.settings['WEIGHT_TEXT_SCORE']
        self.wps = self.settings['WEIGHT_PAGE_SCORE']
        self.wls = self.settings['WEIGHT_LINK_SURROUNDINGS']
        self.wud = self.settings['WEIGHT_UNVISITED_DOMAINS']
        self.wrvd = self.settings['WEIGHT_RELEVANT_VISITED_DOMAINS']
        if self.settings.get("PRIORITY") == "unvisited_domains":
            self.scoring_procedure = self._unvisited_domains
        elif  self.settings.get("PRIORITY") == "bfs":
            self.scoring_procedure = self._bfs
        elif  self.settings.get("PRIORITY") == "link_surroundings":
            self.scoring_procedure = self._link_surroundings
        elif  self.settings.get("PRIORITY") == "link_domain_combined":
            self.scoring_procedure = self._link_domain_combined
        elif self.settings.get("PRIORITY") == "antbased":
            self.scoring_procedure = self._antbased
        else:
            logging.error("SETTINGS ERROR: Not a valid priority defined")
            return

    #################################################################
    #
    #   process_item: metodo
    #       IN:     item, scrapy.Item
    #               spider, scrapy.Spider
    #
    #       Procesa cada objeto generado por una instancia de scrapy.Spider.
    #       En particular, hace uso de una prioridad para valorar el score
    #       de acuerdo a su estrategia
    #
    #################################################################
    def process_item(self, item, spider):
        page = item['page']
        url = page['page_url']
        self.mydb = pymysql.connect(host=self.settings['MYSQL_HOST'],
                                     user=self.settings['MYSQL_USER'],
                                     password=self.settings['MYSQL_PASSWORD'],
                                     database=self.settings['MYSQL_DB'],
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        tx = self.mydb.cursor()
        logging.info("Scoring Links for " + url)
        self.scoring_procedure(tx, item)
        self.mydb.commit()

    #################################################################
    #
    #   _unvisited_domains: metodo
    #       IN:     tx, cursor de pymysql
    #               item, Page. Representa la página a almacenar
    #
    #       Valora y almacena la valoración de un enlace
    #       de acuerdo a la prioridad Unvisited Domains
    #
    #################################################################
    def _unvisited_domains(self, tx, item):
        sql_select_domain = 'SELECT found_hate_pages, found_pages ' \
                      'FROM Domain ' \
                      'WHERE domain = %s'
        sql_update = "UPDATE Link L " \
                      "SET score = %s " \
                      "WHERE domain = %s "
        hate_pages = 0
        total_pages = 0
        try:
            tx.execute(sql_select_domain, item['page']['domain'])
            res = tx.fetchone()
            if res is not None:
                hate_pages = res['found_hate_pages']
                total_pages = res['found_pages']
            irrelevant_pages = total_pages-hate_pages
            if irrelevant_pages < self.settings['MIN_VISISTED_PAGES']:
                score = 1
            else:
                score = 1/(irrelevant_pages - self.settings['MIN_VISISTED_PAGES'] + 1)
            tx.execute(sql_update, (score, item['page']['domain']))
        except Exception as e:
            print(str(e))
            print(item)

        for link in item['links']:
            sql_select_domain = 'SELECT found_hate_pages, found_pages ' \
                          'FROM Domain ' \
                          'WHERE domain = %s'
            sql_update = "UPDATE Link L " \
                          "SET score = %s " \
                          "WHERE link_url = %s "
            hate_pages = 0
            total_pages = 0

            try:
                tx.execute(sql_select_domain, link['domain'])
                res = tx.fetchone()
                if res is not None:
                    hate_pages = res['found_hate_pages']
                    total_pages = res['found_pages']
                irrelevant_pages = total_pages-hate_pages
                if irrelevant_pages < self.settings['MIN_VISISTED_PAGES']:
                    score = 1
                else:
                    score = 1/(irrelevant_pages - self.settings['MIN_VISISTED_PAGES'] + 1)

                tx.execute(sql_update, (score, link['link_url']))
            except Exception as e:
                print(str(e))
                print(item)
                continue

    #################################################################
    #
    #   _link_surroundings: metodo
    #       IN:     tx, cursor de pymysql
    #               item, Page. Representa la página a almacenar
    #
    #       Valora y almacena la valoración de un enlace
    #       de acuerdo a la prioridad Link Surroundings
    #
    #################################################################
    def _link_surroundings(self, tx, item):
        for link in item['links']:
            sql_select_1 = """SELECT page_url, id_in_page 
                               FROM Link_FoundIn
                               WHERE link_url = %s """
            sql_select_2 = """SELECT score 
                           FROM Text as text
                           WHERE text.page_url=%s AND text.id_in_page=%s """
            sql_select_3 = """SELECT score 
                           FROM Page as page
                           WHERE page.page_url=%s"""
            sql_update = """UPDATE Link L
                          SET score = %s
                          WHERE link_url = %s"""

            try:
                tx.execute(sql_select_1, link['link_url'])
                res = tx.fetchall()
                text_score = 0
                page_score = 0
                if len(res) == 0:
                    print("ERROR: scoring link without founding it")
                    continue
                for ocurrence in res:
                    tx.execute(sql_select_2, (ocurrence['page_url'], ocurrence['id_in_page']))
                    res2 = tx.fetchone()
                    text_score += res2['score']
                    tx.execute(sql_select_3, ocurrence['page_url'])
                    res2 = tx.fetchone()
                    page_score += res2['score']
                score = (self.wts*text_score + self.wps*page_score)/len(res)*(self.wts+self.wps)
                tx.execute(sql_update, (score, link['link_url']))
            except Exception as e:
                print(str(e))
                print(item)
                continue



    #################################################################
    #
    #   _link_domain_combined: metodo
    #       IN:     tx, cursor de pymysql
    #               item, Page. Representa la página a almacenar
    #
    #       Valora y almacena la valoración de un enlace
    #       de acuerdo a la prioridad Link Domain Combined
    #
    #################################################################
    def _link_domain_combined(self, tx, item):
        sql_select_links = 'SELECT link_url, domain ' \
                      'FROM Link ' \
                      'WHERE domain = %s'
        tx.execute(sql_select_links, item['page']['domain'])
        links2 = list(tx.fetchall())
        for link in item['links']+links2:
            sql_select_1 = """SELECT page_url, id_in_page 
                               FROM Link_FoundIn
                               WHERE link_url = %s """
            sql_select_2 = """SELECT score 
                           FROM Text as text
                           WHERE text.page_url=%s AND text.id_in_page=%s """
            sql_select_3 = """SELECT score 
                           FROM Page as page
                           WHERE page.page_url=%s"""
            sql_select_4 = 'SELECT COUNT(page_url) AS hate_pages ' \
                          'FROM Page ' \
                          'WHERE domain = %s AND score > %s'
            sql_select_5 = "SELECT COUNT(page_url) AS total_pages " \
                          "FROM Page " \
                          "WHERE domain = %s "
            sql_update = "UPDATE Link L " \
                          "SET score = %s " \
                          "WHERE link_url = %s "

            hate_pages = 0
            total_pages = 0
            try:
                tx.execute(sql_select_1, link['link_url'])
                res = tx.fetchall()
                text_score = 0
                page_score = 0
                if len(res) == 0:
                    print("ERROR: scoring link without founding it")
                    continue
                for ocurrence in res:
                    tx.execute(sql_select_2, (ocurrence['page_url'], ocurrence['id_in_page']))
                    res2 = tx.fetchone()
                    text_score += res2['score']
                    tx.execute(sql_select_3, ocurrence['page_url'])
                    res2 = tx.fetchone()
                    page_score += res2['score']
                link_score = (self.wts*text_score + self.wps*page_score)/len(res)*(self.wts+self.wps)

                tx.execute(sql_select_4, (link['domain'], self.settings['THRESHOLD']))
                res = tx.fetchone()
                if res is not None:
                    hate_pages = res['hate_pages']
                tx.execute(sql_select_5, link['domain'])
                res = tx.fetchone()
                if res is not None:
                    total_pages = res['total_pages']
                irrelevant_pages = total_pages-hate_pages
                if irrelevant_pages < self.settings['MIN_VISISTED_PAGES']:
                    domain_score = 1
                else:
                    domain_score = 1/(irrelevant_pages - self.settings['MIN_VISISTED_PAGES'] + 1)

                if hate_pages < self.settings['MAX_RELEVANT_PAGES']:
                    domain_score_2 = 1
                else:
                    domain_score_2 = (1 / (hate_pages - self.settings['MAX_RELEVANT_PAGES'] + 1))**0.5
                score = (self.wls*link_score + self.wud*domain_score + self.wrvd*domain_score_2)/(self.wud+self.wls+self.wrvd)
                tx.execute(sql_update, (score, link['link_url']))
            except Exception as e:
                print(str(e))
                print(item)
                continue

    #################################################################
    #
    #   _antbased: metodo
    #       IN:     tx, cursor de pymysql
    #               item, Page. Representa la página a almacenar
    #
    #       Valora y almacena la valoración de un enlace
    #       de acuerdo a la prioridad Antbased
    #
    #################################################################
    def _antbased(self, tx, item):
        link_path = item['link_path']
        score = item['page']['score']
        for round in range(0, len(link_path)-1):
            try:
                sql_update = "UPDATE Link_FoundIn SET score = %s*score + %s "  \
                             "WHERE page_url = %s " \
                             "AND link_url = %s "
                tx.execute(sql_update, (self.settings['LOSS'], score, link_path[round], link_path[round+1]))
            except Exception as e:
                print(str(e))
                print(link_path[round] + " and " + link_path[round+1])
                continue
        return


    #################################################################
    #
    #   _bfs: metodo
    #       IN:     tx, cursor de pymysql
    #               item, Page. Representa la página a almacenar
    #
    #       Valora y almacena la valoración de un enlace
    #       de acuerdo a la prioridad BFS
    #
    #################################################################
    def _bfs(self, tx, item):
        return


