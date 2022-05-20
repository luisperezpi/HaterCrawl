
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
import mysql.connector
import time
import logging

from HaterCrawl.spiders.HaterCrawlSpider import HaterCrawlSpider
import getopt, sys
import os
import csv

def create_db(settings):
    mydb = mysql.connector.connect(
        host=settings['MYSQL_HOST'],
        user=settings['MYSQL_USER'],
        password=settings['MYSQL_PASSWORD'],
        port=settings['MYSQL_PORT']
    )
    sql = "SHOW DATABASES LIKE \"" + settings['MYSQL_DB'] + "\";"
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(sql)
    res = mycursor.fetchone()
    if res is None:
        sql = "CREATE DATABASE " + settings['MYSQL_DB']
        mycursor.execute(sql)
        sql = "ALTER DATABASE " + settings['MYSQL_DB'] + \
                " CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;"
        mycursor.execute(sql)
        mydb.commit()
        sql = "USE " + settings['MYSQL_DB']
        mycursor.execute(sql)
        mydb.commit()
        sql = """CREATE TABLE Domain (domain VARCHAR(256) NOT NULL PRIMARY KEY, 
              			found_hate_pages INT, 
              			found_pages INT, 
                        discovered_time DATETIME, 
                        last_seen_time DATETIME, 
              			INDEX found_desc (found_hate_pages DESC));"""
        mycursor.execute(sql)
        sql = """CREATE TABLE Page (page_url VARCHAR(512) PRIMARY KEY, 
          			title VARCHAR(512), 
         			score FLOAT(24), 
          			found_hate_texts INT, 
          			domain VARCHAR(256),
          			time DATETIME, 
          			deep_web BOOL,
                    INDEX score_desc (score DESC),
                    INDEX time_desc (time DESC),
          			FOREIGN KEY (domain) REFERENCES Domain(domain));"""
        mycursor.execute(sql)
        sql = """CREATE TABLE Text ( id_in_page int, 
                    page_url VARCHAR(512), 
                    text MEDIUMTEXT, 
                    label VARCHAR(128), 
                    score FLOAT(24), 
                    INDEX score_desc (score DESC),
                    FOREIGN KEY (page_url) REFERENCES Page(page_url), 
                    PRIMARY KEY (id_in_page, page_url));"""
        mycursor.execute(sql)
        sql = """CREATE TABLE Link ( link_url VARCHAR(512) PRIMARY KEY,
                  score FLOAT(24), 
                  visited BOOL, 
                  domain VARCHAR(256), 
                  found_n INT, INDEX score_desc (score DESC));"""
        mycursor.execute(sql)
        sql = """CREATE TABLE Link_FoundIn ( id_in_page INT, 
                          page_url VARCHAR(512), 
                          link_url VARCHAR(512), 
                          score FLOAT(24),
                          visited BOOL, 
                          INDEX score_desc (score DESC),
                          FOREIGN KEY (link_url) REFERENCES Link(link_url), 
                          FOREIGN KEY (id_in_page, page_url) REFERENCES Text(id_in_page, page_url));"""
        mycursor.execute(sql)
        mydb.commit()
        mycursor.close()
        mydb.close()
    else:
        mycursor.close()
        mydb.close()
        return


def recopilate_data(settings):
    stats = settings['RESULTS_FILE']
    with open(stats, "w", newline='') as f:
        logging.info("Comenzando toma de resultados.")
        logging.info("Intervalos: " + str(settings['NUM_INTERVALS']))
        logging.info("Duración: " + str(settings['WAIT_INTERVAL']))
        writer = csv.writer(f)
        writer.writerow(['iteraciones', 'total_domains', 'total_pages', 'deep_pages', 'hate_pages', 'hate_deep_pages', 'hate_domains', 'avg_score_rel', 'avg_score'])
        writer.writerow([0, 0, 0, 0, 0, 0, 0, 0, 0])

    for i in range(1, settings['NUM_INTERVALS'] + 1):
        time.sleep(settings['WAIT_INTERVAL'])
        mydb_snapshot = mysql.connector.connect(
            host=settings['MYSQL_HOST'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            database=settings['MYSQL_DB'],
            port=settings['MYSQL_PORT']
        )
        mycursor = mydb_snapshot.cursor(prepared=True)
        with open(stats, "a", newline='') as f:
            writer = csv.writer(f)
            results = []
            results.append(i)
            sql_sel = "SELECT COUNT(*) AS total_domains FROM Domain;"
            mycursor.execute(sql_sel)
            res = mycursor.fetchone()
            results.append(res[0])
            sql_sel = "SELECT COUNT(*) AS total_pages FROM Page;"
            mycursor.execute(sql_sel)
            res = mycursor.fetchone()
            results.append(res[0])
            sql_sel = "SELECT COUNT(*) AS deep_pages FROM Page WHERE deep_web=true;"
            mycursor.execute(sql_sel)
            res = mycursor.fetchone()
            results.append(res[0])
            sql_sel = "SELECT COUNT(*) AS hate_pages FROM Page WHERE score>%s AND found_hate_texts>0;"
            mycursor.execute(sql_sel, (settings['THRESHOLD'],))
            res = mycursor.fetchone()
            results.append(res[0])
            sql_sel = "SELECT COUNT(*) AS hate_deep_pages FROM Page WHERE score>%s AND found_hate_texts>0 AND deep_web=true;"
            mycursor.execute(sql_sel, (settings['THRESHOLD'],))
            res = mycursor.fetchone()
            results.append(res[0])
            sql_sel = "SELECT COUNT(DISTINCT(domain)) AS hate_domains FROM Page WHERE score>%s AND found_hate_texts>0;"
            mycursor.execute(sql_sel, (settings['THRESHOLD'],))
            res = mycursor.fetchone()
            results.append(res[0])
            sql_sel = "SELECT AVG(score) AS avg_score FROM Page WHERE score>%s AND found_hate_texts>0;"
            mycursor.execute(sql_sel, (settings['THRESHOLD'],))
            res = mycursor.fetchone()
            results.append(res[0])
            sql_sel = "SELECT AVG(score) AS avg_score FROM Page;"
            mycursor.execute(sql_sel)
            res = mycursor.fetchone()
            results.append(res[0])
            writer.writerow(results)
        mycursor.close()
        mydb_snapshot.close()

    logging.info("Fin de la toma de resultados.")
    reactor.stop()

def stop_reactor(settings):
    time.sleep(settings['NUM_INTERVALS']*settings['WAIT_INTERVAL'])
    logging.info("Fin de la ejecucion.")
    reactor.stop()


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hn:s:r:d:p:l:m:", [ "name=", "seeds=", "results=", "database=", "priority=", "inc_w=", "inc_n=", "log=", "model="])
    except getopt.GetoptError:
        print("BAD USE")
        sys.exit(2)
    settings = get_project_settings()
    if not os.path.isdir("logs"):
        os.makedirs("logs")
    if not os.path.isdir("results"):
        os.makedirs("results")

    for opt, arg in opts:
        if opt == '-h':
            print("HELP")
            sys.exit()
        elif opt in ("-n", "--name"):
            settings['RESULTS_FILE'] = "results/" + arg + ".csv"
            settings['LOG_FILE'] = "logs/" + arg + ".txt"
            settings['MYSQL_DB'] = arg
        elif opt in ("-s", "--seeds"):
            settings['SEED_FILE'] = arg
        elif opt in ("-r", "--results"):
            settings['RESULTS_FILE'] = "results/" + arg
        elif opt in ("-d", "--database"):
            settings['MYSQL_DB'] = arg
        elif opt in ("-p", "--priority"):
            settings['PRIORITY'] = arg
        elif opt == "--inc_w":
            settings['WAIT_INTERVAL'] = int(arg)
        elif opt == "--inc_n":
            settings['NUM_INTERVALS'] = int(arg)
        elif opt in ("-l", "--log"):
            settings['LOG_FILE'] = "logs/" + arg
        elif opt in ("-m", "--model"):
            settings['MODEL_DIR'] = arg

    create_db(settings)
    configure_logging()
    runner = CrawlerRunner(settings)
    d = runner.crawl(HaterCrawlSpider)
    d.addBoth(lambda _: reactor.stop())
    reactor.callInThread(recopilate_data, settings)
    reactor.run() # the script will block here until the crawling is finished

if __name__ == "__main__":
   main(sys.argv[1:])