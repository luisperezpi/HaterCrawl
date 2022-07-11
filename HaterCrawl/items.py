# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Field, Item
from itemloaders.processors import TakeFirst, MapCompose, Join, Identity
from w3lib.html import remove_tags
import tldextract
import urllib.parse as urlparse


class Output(Item):
    page = Field()
    short_texts = Field()
    long_texts = Field()
    links = Field()
    link_path = Field()
    links_foundin = Field()

def empty_title(title):
    if title is None:
        return "Empty"
    return title

def get_domain(url):
    ext = tldextract.extract(url)
    if ext.domain is None or len(ext.domain) == 0:
        return "DOMAIN VACIO"
    return ext.domain;

def get_web(url):
    if "onion" in url:
        return 1
    else:
        return 0

def url_fix(url):
    scheme, netloc, path, qs, anchor = urlparse.urlsplit(url)
    path = urlparse.quote(path, '/%')
    qs = urlparse.quote_plus(qs, ':&=')
    return urlparse.urlunsplit((scheme, netloc, path, qs, anchor))

class Page(Item):
    page_url = Field(
        input_processor=MapCompose(str.lower),
        output_processor=TakeFirst(),
    )
    domain = Field(
        input_processor=MapCompose(get_domain, str.lower),
        output_processor=TakeFirst(),
    )
    title = Field(
        input_processor=MapCompose(empty_title, remove_tags),
        output_processor=TakeFirst(),
    )
    source = Field(
        input_processor=Identity(),
        output_processor=TakeFirst(),
    )
    deep_web = Field(
        input_processor=MapCompose(get_web),
        output_processor=TakeFirst(),
    )
    score = Field()
    score_sqrt = Field()
    score_avg = Field()
    found_hate_texts = Field()

class Text(Item):
    id_in_page = Field(
        input_processor=MapCompose(int),
        output_processor=TakeFirst(),
    )
    page_url = Field(
        input_processor=MapCompose(str.lower),
        output_processor=TakeFirst(),
    )
    text = Field(
        input_processor=MapCompose(),
        output_processor=TakeFirst(),
    )
    label = Field()
    score = Field()

class Link(Item):
    link_url = Field(
        input_processor=MapCompose(),
        output_processor=TakeFirst(),
    )
    domain = Field(
        input_processor=MapCompose(get_domain, str.lower),
        output_processor=TakeFirst(),
    )
    score = Field()

class Link_FoundIn(Item):
    page_url = Field(
        input_processor=MapCompose(str.lower),
        output_processor=TakeFirst(),
    )
    id_in_page = Field(
        input_processor=MapCompose(int),
        output_processor=TakeFirst(),
    )
    link_url = Field(
        input_processor=MapCompose(),
        output_processor=TakeFirst(),
    )

