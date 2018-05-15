import requests
import redis
import brotli
import queue
import threading
import os
from multiprocessing.dummy import Pool

base = 'http://23.95.221.108'
cores = 8
pool = Pool(cores)
limit = 1290
redis_queue = queue.LifoQueue()
client = redis.StrictRedis()
cache = client.hgetall("ebooks")
dir_name = './txt/'


def get(path):
    url = base + path
    if cache.get(path) is not None:
        html = brotli.decompress(cache[url])
    else:
        html = requests.get(url).text
        redis_queue.put((path, brotli.compress(html.encode(), brotli.MODE_TEXT)))

    return html


def page(page_id):
    path = page_path(page_id)
    return path, get(path)


def page_path(page_id):
    return '/page/' + str(page_id)


def get_page(page_id):
    path, html = page(page_id)
    fd = open(dir_name + path + '.txt')
    fd.write(html)
    fd.flush()
    fd.close()


def redis_set():
    while redis_queue.not_empty:
        key, val = redis_queue.get()
        print(f'{key} -> {len(val)}')
        client.hset("ebooks", key, val)


def main():
    print("Got %d items" % len(cache))
    if not os.path.exists(dir_name):
        print("Creating dir %s" % dir_name)
        os.makedirs(dir_name)

    threading.Thread(target=redis_set, daemon=True).start()
    pool.map_async(get_page, range(1, limit + 1)).wait()


main()
