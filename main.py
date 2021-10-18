import io
import json
import boto3
import random
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from newspaper import Article

from webshooter import StaticScraper, url_to_soup

import logging
logging.root.setLevel(logging.INFO)


config = {}
app = StaticScraper(progbar=True)


def load_json(path):
    with open(path, 'r') as f:
        data = json.load(f)
    return data

def get_yesterday():
    korean_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=9)
    korean_time = korean_time + datetime.timedelta(days=-1)
    date = korean_time.strftime('%Y%m%d')
    return date


def get_url_from_id(domain_id, date, page):
    url = f'https://news.naver.com/main/list.naver?mode=LSD&mid=sec&sid1={domain_id}&listType=title&date={date}&page={page}'
    return url

def get_url_from_keyword(keyword, date, page):
    query = ' | '.join(keyword)
    start = (page - 1) * 10 + 1
    url = f'https://search.naver.com/search.naver?where=news&query={query}&sort=0&nso=p:from{date}to{date}&start={start}'
    return url

def find_max_page_from_id(domain_id, date):
    page_url = get_url_from_id(domain_id, date, 10000)
    soup = url_to_soup(page_url)
    max_page = soup.select('div.paging strong')[0].text
    return int(max_page)

def find_max_page_from_keyword(keyword, date):
    page_url = get_url_from_keyword(keyword, date, 10000)
    soup = url_to_soup(page_url)
    max_page = soup.select('div.sc_page_inner a')[-1].text
    return int(max_page)

def get_meta_from_id_page(url):
    soup = url_to_soup(url)
    articles = soup.select('div.list_body ul.type02 a')
    titles = [i.text for i in articles]
    urls = [i.get('href') for i in articles]
    authors = [i.text for i in soup.select('div.list_body ul.type02 span.writing')]
    return [{'title': t, 'url': u, 'author': a} for t, u, a in zip(titles, urls, authors)]

def get_meta_from_keyword_page(url):
    soup = url_to_soup(url)
    news_list = soup.select('div.news_area')
    titles = [n.select('a.news_tit')[0].get('title') for n in news_list]
    urls = [n.select('a.news_tit')[0].get('href') for n in news_list]
    authors = [n.select('div.info_group a')[0].text.strip() for n in news_list]
    return [{'title': t, 'url': u, 'author': a} for t, u, a in zip(titles, urls, authors)]


@app.register('browse')
def browse():
    results = []

    for domain in app.v.domain:
        domain_results = []

        if domain['id'] is not None:
            url = get_url_from_id(domain['id'], app.v.date, 1)
            max_page = find_max_page_from_id(domain['id'], app.v.date)
            
            for p in range(1, max_page+1):
                p_url = get_url_from_id(domain['id'], app.v.date, p)
                meta = get_meta_from_id_page(p_url)
                meta = [{'domain': domain['name'], 'date': app.v.date, **m} for m in meta]
                domain_results += meta
        
        if domain['keyword'] is not None:
            url = get_url_from_keyword(domain['keyword'], app.v.date, 1)
            max_page = find_max_page_from_keyword(domain['keyword'], app.v.date)

            for p in range(1, max_page+1):
                p_url = get_url_from_keyword(domain['keyword'], app.v.date, p)
                meta = get_meta_from_keyword_page(p_url)
                meta = [{'domain': domain['name'], 'date': app.v.date, **m} for m in meta]
                domain_results += meta

        domain_results = random.sample(domain_results, min(app.v.num_articles, len(domain_results)))
        results += domain_results

    return results


@app.register('parse', multiprocess=True)
def parse(html):
    article = Article('', language='ko', fetch_images=False)
    article.download(html)
    article.parse()
    text = article.text
    return {'text': text}


def save_on_s3(data, name):
    s3 = boto3.client('s3', aws_access_key_id=app.v.aws_access_key_id, aws_secret_access_key=app.v.aws_secret_access_key)
    buffer = io.StringIO()
    data.to_csv(buffer, index=False)
    res = s3.put_object(
        Body = buffer.getvalue().encode('utf-8-sig'),
        Bucket = app.v.aws_s3_bucket,
        Key = f'{name}.csv'   
    )

def main():
    config = load_json('config.json')
    app.set_vars(config)
    date = get_yesterday()
    app.set_var('date', date)

    data = app.run()
    data = pd.read_csv('data.csv')
    save_on_s3(data, app.v.date)


if __name__ == '__main__':
    main()