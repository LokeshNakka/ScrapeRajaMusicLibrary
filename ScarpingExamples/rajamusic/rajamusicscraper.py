import math
import os
import queue
import threading
import time

import requests
import json

from lxml import html
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

ua = UserAgent()
useragent = ua.random

headr = {'User-Agent': useragent}
movies_links_list = []


def GenMovieLinks():
    global movies_links_list
    movie_links = queue.Queue()

    def Thread_req(url):
        while True:
            response = requests.get(url, headers=headr)
            if response.status_code == 200:
                hrefs = html.fromstring(response.text).xpath('//div[@id="persons-image1"]/a/@href')
                for href in hrefs:
                    movie_links.put(href)
                break
            else:
                time.sleep(.5)
                headr['User-Agent'] = ua.random


    threads = []
    for i in tqdm(range(0, 841, 12),desc='GenMovieLinks'):
        url = f'https://rajamusicbank.com/movie_index.php?action=list&alpbet=&start={i}'
        t = threading.Thread(target=Thread_req, args=(url, ))
        t.start()
        threads.append(t)
        time.sleep(.3)

    for thread in threads:
        thread.join()

    with open('movies_links.txt', 'w') as file:
        length = movie_links.qsize()
        movies_links_list = [movie_links.get() for i in range(length)]
        file.write('\n'.join(movies_links_list))


def MovieSongsLinks():
    global movies_links_list
    threads = []
    movies_details = queue.Queue()
    if len(movies_links_list) == 0:
        with open('movies_links.txt', 'r') as file:
            movies_links_list = file.read().split('\n')

    def Thread_req(url):
        global headr
        while True:
            response = requests.get(url, headers=headr)
            if response.status_code == 200:
                tree = html.fromstring(response.text)
                data = {'movie_name': '_'.join(tree.xpath('//div[@id="persons-image"]/descendant::text()'))
                    .replace('\n_', '').replace('\n', ''),
                        'image': ''.join(tree.xpath('//div[@id="persons-image"]/descendant::img/@src')),
                        'title': ''.join(tree.xpath('//div[@id="persons-image"]/descendant::img/@title'))
                            .replace('/', '').replace('\\', '').strip()}

                songs_tree_list = tree.xpath('//div[@id="songs-list"][position()>1]/descendant::a')
                data['songs_urls'] = [''.join(record.xpath('@href')) for record in songs_tree_list]
                movies_details.put(data)

                movie_path = f"data/{data['title']}"
                if not os.path.exists(movie_path):
                    os.makedirs(movie_path)
                img_response = requests.get(data['image'], headers=headr)
                if img_response.status_code == 200:
                    with open(f'{movie_path}/img.{data["image"].split(".")[-1]}', 'wb') as img:
                        img.write(img_response.content)
                with open(f'{movie_path}/data.json', 'w') as file:
                    json.dump(data, file)
                break
            else:
                time.sleep(.5)
                headr = {'User-Agent': ua.random}
                print('Error ', url)

    for url in tqdm(movies_links_list, desc='MovieSongsLinks'):
        t = threading.Thread(target=Thread_req, args=(url,))
        t.start()
        threads.append(t)
        time.sleep(.4)

    for thread in threads:
        thread.join()


drive_path = ''


def GetSong_threaded_Sel(pos, movies_list, bar):
    global drive_path
    driver = webdriver.Chrome(drive_path)
    driver.set_window_size(300, 700)
    driver.set_window_position(pos * 150, 0)
    for index, movie in enumerate(movies_list):
        if movie.strip()[-3:] == 'inf':
            bar.update(1)
            continue
        with open(f'data/{movie}/data.json', 'r') as file:
            jdata = json.load(file)
            for url in jdata['songs_urls']:
                driver.get(url)
                try:
                    WebDriverWait(driver, 3).until(EC.presence_of_all_elements_located(
                        (By.XPATH, '//span[@id="get_english_lyrics_id"]/child::*')))
                    WebDriverWait(driver, 3).until(EC.presence_of_all_elements_located(
                        (By.XPATH, '//span[@id="get_telugu_lyrics_id"]/child::*')))
                    song = {'songTitle': driver.find_element(By.XPATH, '//div[@id="songs-list12"]').text,
                            'details': driver.find_elements(By.XPATH, '//div[@id="songs-list"]')[3].text,
                            'telugu_lyric': driver.find_element(By.XPATH,
                                                                '//span[@id="get_telugu_lyrics_id"]').text,
                            'english_lyric': driver.find_element(By.XPATH,
                                                                 '//span[@id="get_english_lyrics_id"]').text}
                    file_name = song['songTitle'].split('»')[-1].split('/')[0].replace('\\', '').replace('/',
                                                                                                         '')
                    with open(f'data/{movie}/{file_name}.json', 'w') as song_file:
                        json.dump(song, song_file)
                except Exception as e:
                    result = driver.find_element(By.XPATH, '//head/title').text
                    # res = requests.get(url, headers=headr)
                    if result.__contains__('404'):  # res.status_code == 404:
                        # Not found
                        pass
        bar.update(1)


def GetSongs(workers=8):
    global drive_path
    threads = []
    drive_path = ChromeDriverManager().install()
    data = os.listdir(f'{os.getcwd()}/data')
    total_work = len(data)
    workLoad = math.ceil(total_work / workers)
    with tqdm(total=len(data), desc='GetSongs') as bar:
        for i in range(workers):
            start = i * workLoad
            end = (i + 1) * workLoad if (i + 1) * workLoad < total_work else total_work
            t = threading.Thread(target=GetSong_threaded_Sel, args=(i, data[start:end], bar))
            t.start()
            threads.append(t)
        for thread in threads:
            thread.join()


GenMovieLinks()
MovieSongsLinks()
GetSongs(6)
