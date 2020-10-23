import os
import re
import json
import time
import base64
import requests
import logging
from urllib.request import urlopen

from datetime import datetime
from bs4 import BeautifulSoup
from datetime import datetime

from models import DEFAULT_DOCUMENT

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SITE_NAME = os.getenv('SITE_NAME')
BASE_URL = os.getenv('BASE_URL')
reg = re.compile(r'[,(文字)]')


def get_html(url):
    res = requests.get(url)
    if res.status_code == 200:
        return res.content
    else:
        return None

        
def create_id(url):
    id_ = SITE_NAME + "-" + url.split('/')[-1]
    return id_

def utc_str2ts_epoch_milli(utc, format="%Y-%m-%d %H:%M:%S"):
    dt = datetime.strptime(utc + "+0000", format + "%z")
    ts = dt.timestamp() * 1000
    return ts

def extract_attributes(html):
    document = DEFAULT_DOCUMENT
    soup = BeautifulSoup(html, "html.parser")
    document['title'] = soup.find("h1", id="workTitle").get_text().strip()
    document['author'] = soup.find("span", id="workAuthor-activityName").get_text().strip()
    try:
        document['genre'] = soup.find("dd", itemprop="genre").get_text().strip()
    except:
        pass
    document['tag'] = [t.get_text().strip() for t in soup.findAll("span", itemprop="keywords")]
    try:
        d1 = soup.find("span", id="catchphrase-body").get_text().strip()
        d2 = soup.find("p", id="introduction").get_text().strip()
        document['description'] = d1 + "¥n¥n" + d2
    except:
        document['description'] = ""
    document['created_at'] = utc_str2ts_epoch_milli(soup.find("time", itemprop="datePublished").get("datetime"), format="%Y-%m-%dT%H:%M:%SZ")
    document['updated_time'] = utc_str2ts_epoch_milli(soup.find("time", itemprop="dateModified").get("datetime"), format="%Y-%m-%dT%H:%M:%SZ")
    document['like_count'] = int(re.sub(reg, "", soup.find("dl", class_="widget-credit").findAll("dd")[-3].get_text()))
    document['length'] = int(soup.find("span", class_="js-follow-button-follower-counter").get("data-follower-count"))
    return document

def lambda_handler(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    logger.info(f'event {event}')
    url = BASE_URL + event['url']
    logger.info(f'scrape {url}')
    logger.info('BEGIN scraping')
    html = get_html(url)
    html = html.decode("utf-8")
    logger.info('END scraping')
    document = extract_attributes(html)
    id_ = create_id(url)
    document['key'] = id_
    document['url'] = url
    document['site_name'] = SITE_NAME
    res = {
        "document": document,
        "id": id_
        }
    return res