#coding:utf-8

import time
from lxml import etree
import urllib.request as request

url = 'http://www.quanjing.com/creative/SearchCreative.aspx?id=7'

def download_one_pic(url:str,name:str,suffix:str='jpg'):
    path = '.'.join([name,suffix])
    response = request.urlopen(url)
    wb_data = response.read()
    with open(path,'wb') as f:
        f.write(wb_data)

def download_many_pic(urls:list):
    start = time.time()
    for i in urls:
        ts = 'test\\'+str(int(time.time() * 1000))
        download_one_pic(i, ts)
    end = time.time()
    print(u'下载完成,%d张图片,耗时:%.2fs' % (len(urls), (end - start)))

def get_pic_src(url:str)->list:
    response = request.urlopen(url)
    wb_data = response.read()
    html = etree.HTML(wb_data)
    pic_urls = html.xpath('//a[@class="item lazy"]/img/@src')
    return pic_urls

def allot(pic_urls:list,n:int)->list:
    #根据给定的组数，分配url给每一组
    _len = len(pic_urls)
    base = int(_len / n)
    remainder = _len % n
    groups = [pic_urls[i * base:(i + 1) * base] for i in range(n)]
    remaind_group = pic_urls[n * base:]
    for i in range(remainder):
        groups[i].append(remaind_group[i])
    return [i for i in groups if i]

#----------------------------------------------------------------------
#同步爬虫

def crawler():
    #同步下载
    pic_urls = get_pic_src(url)
    download_many_pic(pic_urls)

#----------------------------------------------------------------------
#多进程爬虫

from multiprocessing.pool import Pool
def multiprocess_crawler(processors:int):
    #多进程爬虫
    pool = Pool(processors)
    pic_urls = get_pic_src(url)
    url_groups = allot(pic_urls,processors)
    for i in url_groups:
        pool.apply_async(func=download_many_pic,args=(i,))
    pool.close()
    pool.join()

#----------------------------------------------------------------------
#多线程爬虫

import random
from threading import Thread

def run_multithread_crawler(pic_urls:list,threads:int):
    begin = 0
    start = time.time()
    while 1:
        _threads = []
        urls = pic_urls[begin:begin+threads]
        if not urls:
            break
        for i in urls:
            ts = 'test\\'+str(int(time.time()*10000))+str(random.randint(1,100000))
            t = Thread(target=download_one_pic,args=(i,ts))
            _threads.append(t)
        for t in _threads:
            t.setDaemon(True)
            t.start()
        for t in _threads:
            t.join()
        begin += threads
    end = time.time()
    print(u'下载完成,%d张图片,耗时:%.2fs' % (len(pic_urls), (end - start)))

def multithread_crawler(threads:int):
    pic_urls = get_pic_src(url)
    run_multithread_crawler(pic_urls,threads)


#----------------------------------------------------------------------
#异步协程爬虫

import asyncio
from asyncio import Semaphore
from aiohttp import ClientSession,TCPConnector

async def download(session:ClientSession,url:str,name:str,sem:Semaphore,suffix:str='jpg'):
    path = '.'.join([name,suffix])
    async with sem:
        async with session.get(url) as response:
            wb_data = await response.read()
            with open(path,'wb') as f:
                f.write(wb_data)

async def run_coroutine_crawler(pic_urls:list,concurrency:int):
    # 异步协程爬虫,最大并发请求数concurrency
    tasks = []
    sem = Semaphore(concurrency)
    conn =TCPConnector(limit=concurrency)
    async with ClientSession(connector=conn) as session:
        for i in pic_urls:
            ts = 'test\\' + str(int(time.time() * 10000)) + str(random.randint(1, 100000))
            tasks.append(asyncio.create_task(download(session,i,ts,sem)))
        start = time.time()
        await asyncio.gather(*tasks)
        end = time.time()
        print(u'下载完成,%d张图片,耗时:%.2fs' % (len(pic_urls), (end - start)))

def coroutine_crawler(concurrency:int):
    pic_urls = get_pic_src(url)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_coroutine_crawler(pic_urls,concurrency))
    loop.close()


#----------------------------------------------------------------------
#多进程+多线程爬虫

def mixed_process_thread_crawler(processors:int,threads:int):
    pool = Pool(processors)
    pic_urls = get_pic_src(url)
    url_groups = allot(pic_urls,processors)
    for group in url_groups:
        pool.apply_async(run_multithread_crawler,args=(group,threads))
    pool.close()
    pool.join()

#----------------------------------------------------------------------
#多进程+异步协程爬虫

def _coroutine_crawler(pic_urls:list,concurrency:int):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_coroutine_crawler(pic_urls, concurrency))
    loop.close()

def mixed_process_coroutine_crawler(processors:int,concurrency:int):
    pool = Pool(processors)
    pic_urls = get_pic_src(url)
    url_groups = allot(pic_urls, processors)
    for group in url_groups:
        pool.apply_async(_coroutine_crawler, args=(group, concurrency))
    pool.close()
    pool.join()


if __name__ == '__main__':
    crawler()
    multiprocess_crawler(40)
    multithread_crawler(120)
    coroutine_crawler(100)
    mixed_process_thread_crawler(4,50)
    mixed_process_coroutine_crawler(4,50)
