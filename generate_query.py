import os
import re
import time
from datetime import datetime
from pprint import pprint

from multiprocessing import *
import requests
import schedule
from googleapiclient.discovery import build
from googleapiclient.http import build_http
from oauth2client import client, tools, file

from all_constants import SCOPE_SEARCH, CLIENT_SECRETS, TOKEN_YM
from all_section import AllSection
from clasterization import Clasterization
from helping_functions import masked, stemmed, json_work

all_section = AllSection()
get_serp = all_section.xml_river
get_heading = all_section.get_heading
get_frequency = all_section.get_frequency
url_ = ["https://redsale.by/remont/poshiv-odezhdy/poshiv-bryuk",
        "https://redsale.by/remont/poshiv-odezhdy/poshiv-bryuk"]


# def init(l):
#     global lock
#     lock = l


class Queries:
    def __init__(self):
        self.all_section = json_work("other_files/all_section.json", "r")
        self.work_file = []
        self.main_file = json_work("other_files/main.json", "r")
        self.list_links = json_work("other_files/list_links.json", "r")

    # TODO найти метод для получения ключей
    # Получение ключевых фраз из YandexMetrika
    def get_from_ym(self, url):
        id_metrika = "25891700"
        url_ = url
        url = f"https://api-metrika.yandex.net/stat/v1/data/comparison?" \
              f"end-date=2020-03-31&" \
              f"ids={id_metrika}&" \
              f"metrics=ym:pv:pageviews&" \
              f"start-date=2020-03-01&" \
              f"dimension=pagePath=='{url_}'"

        response = requests.get(url=url, headers={"Authorization": f"OAuth {TOKEN_YM}"})

    # Получение ключевых фраз из GSC
    def get_keys_from_gls(self, url):
        request = {
            "startDate": "2020-03-01",
            "endDate": "2020-03-15",
            "dimensions": ["query"],

            "dimensionFilterGroups": [
                {
                    "filters": [
                        {
                            "dimension": "page",
                            "operator": "contains",
                            "expression": url
                        }
                    ]
                }
            ],
            'rowLimit': 25000,
        }

        flow = client.flow_from_clientsecrets(
            CLIENT_SECRETS, scope=SCOPE_SEARCH, message=tools.message_if_missing(CLIENT_SECRETS)
        )

        storage = file.Storage("other_files/webmasters.dat")
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage)
        http = credentials.authorize(http=build_http())
        try:
            service = build("webmasters", "v3", http=http)
            keys_list = []

            result = service.searchanalytics().query(
                siteUrl="https://redsale.by", body=request).execute()
            try:
                for row in result["rows"]:
                    keys_list.append(row["keys"][0])
            except KeyError:
                print(result)

            return keys_list
        except:
            print("Ошибка подключения, переподключаюсь")
            self.get_keys_from_gls(url)

    # Удаление дублей
    def clean_double(self, keys_list):
        tmp_list = []
        idx_list = []

        for item1 in keys_list:
            tmp_list.append(stemmed(item1))

        for idx, item2 in enumerate(tmp_list):
            if tmp_list.count(item2) > 1:
                idx_list.append(idx)
                del tmp_list[idx]
        for idx in set(idx_list):
            del keys_list[idx]

        return keys_list

    # Генерация предтемплейта
    def generate_pretmp(self, keys_list):
        list_tmp = []
        for key in keys_list:
            tmp_queries = {}
            tmp_queries["date"] = datetime.now().date().isoformat()
            tmp_queries["query"] = key
            tmp_queries["maska"] = masked(key)
            without_minsk = tmp_queries["maska"]["without_minsk"]
            tmp_queries["stemming"] = stemmed(without_minsk)
            if self.checkin_stemming(list_tmp, tmp_queries):
                if self.checkin_stemming(self.all_section, tmp_queries):
                    if self.checkin_stemming(self.main_file, tmp_queries):
                        list_tmp.append(tmp_queries)
        # print(list_tmp)
        self.work_file = list_tmp

    # TODO в дальнейшем применить pandas
    # Сравнение двух списков
    @staticmethod
    def find_match(one_list, second_list):
        for num, item in enumerate(one_list):
            stemm_one = item["stemming"]
            for item_ in second_list:
                stemm_two = item_["stemming"]
                if stemm_one == stemm_two:
                    try:
                        one_list.pop(num)
                    except IndexError:
                        if num == 1 and len(one_list) > 0:
                            one_list.pop(0)

    # Окончательная генерация темплейта
    def template_generated(self, item):
        print(item)
        # l = Lock()
        frequency = {}
        maska = item["maska"]
        serp = get_serp(maska["with_minsk"])
        stemming = item["stemming"]
        try:
            basic_freq = get_frequency([maska["with_minsk"]])[0]["Shows"]
            basic_freq += get_frequency([f'{maska["without_minsk"]} цена'])[0]["Shows"]
            accurate_freq = get_frequency([f'"{maska["with_minsk"]}"'])[0]["Shows"]
            accurate_freq += get_frequency([f'"{maska["without_minsk"]} цена"'])[0]["Shows"]
        except TypeError:
            (basic_freq, accurate_freq) = 0, 0
        frequency["basic"] = basic_freq
        frequency["accurate"] = accurate_freq
        item["SERP"] = serp
        item["heading_entry"] = get_heading(serp, stemming)
        item["frequency"] = frequency
        # lock.acquire()
        try:
            work = json_work("other_files/work_file.json", "r")
            work.append(item)
            json_work("other_files/work_file.json", "w", work)
        finally:
            pass
            # lock.release()

    def checkin_main(self, main_file, keys):
        for item in main_file:
            if item["query"] in keys:
                # print(f'запрос {item["query"]} был удален из ключей')
                keys.remove(item["query"])

    # получение ключей из текстового файла
    def get_key_from_txt(self, keys):
        list_keys = []
        with open("other_files/keys_.txt") as f:
            for item in f:
                if item.strip().lower() not in keys:
                    list_keys.append(item.strip().lower())

        return list_keys

    # проверка наличия stemming вне зависимости от последовательности слов
    def checkin_stemming(self, stemmings, item):
        for item_ in stemmings:
            if set(item["stemming"].split(' ')) == set(item_["stemming"].split(' ')):
                #print(f'{item["stemming"]} = {item_["stemming"]}')
                return False
        return True

    # def clean_double_2(self, list_from, list_rm):
    #
    #     for idx1, item1 in enumerate(list_from):
    #         for idx2, item2 in enumerate(list_rm):
    #             count_match = len(set(item1["stemming"].split()) & set(item2["stemming"].split()))
    #             len_stemm = max(len(item1["stemming"].split()), len(item2["stemming"].split()))
    #             # print(F"Кол {count_match} длинна {len_stemm} стемм1 {item1['stemming']} стемм2 {item2['stemming']}")
    #             if count_match == len_stemm or item1["stemming"] == item2["stemming"]:
    #                 list_rm.pop(idx2)
    #
    #     if not self.checkin_double(list_rm):
    #         return self.clean_double_2(self.work_file, self.work_file)
    #
    # def checkin_double(self, list_):
    #     list_1 = [i["stemming"] for i in list_]
    #     for item in list_1:
    #          if list_1.count(item) > 1:
    #              return False
    #
    #     return True

    # Запуск скрипта
    def run(self, url):
        empty = []
        json_work("other_files/work_file.json", "w", empty)
        print(url)
        self.get_from_ym(url)

        keys = self.get_keys_from_gls(url)  # получение ключей gsc
        keys += self.get_key_from_txt(keys)  # получение ключей из файла
        keys = self.clean_double(keys)  # удаление дублей

        if len(keys) > 0:
            self.checkin_main(self.main_file, keys)  # Удаление ключей присутствующих в main_file
            self.generate_pretmp(keys)  # генерация претемплейтов по ключам c уникальным stemming
            for item in self.work_file:
                print(item["maska"]["without_minsk"])

            if len(self.work_file) > 0:
                # l = Lock()
                # p = Pool(initializer=init, initargs=(l,), processes=5)
                # p.map(self.template_generated, self.work_file)  # генерация конечного темплейта
                # p.close()
                # p.join()
                for item in self.work_file:
                    self.template_generated(item)

                main = json_work("other_files/main.json", "r")
                work = json_work("other_files/work_file.json", "r")
                gen_data = main + work
                json_work("other_files/main.json", "w", gen_data)

        print("Завершено")
        # ''' Кластеризуем work '''
        # if len(main) < 0:
        #     return False
        # else:
        #     clasterization = Clasterization(main, url=url)
        #     clasterization.run()
        #     return True

    # работа по 10 url
    def get_claster_with_count(self, count):
        for link in self.list_links[:count]:
            status = self.run(link)
            # print(f"кол-во урлов {count}")
            idx = self.list_links.index(link)
            # print(f"Url для удаления {self.list_links[idx]}")
            # print(f"Длина списка {len(self.list_links)}")
            del self.list_links[idx]
            # print(f"Длина списка {len(self.list_links)}")
            json_work("other_files/list_links.json", "w", self.list_links)
            if not status:
                count += 1

    # запуск скрипта по списку урлов
    def start_for_list_url(self, item):
        url_second_level = re.search(r"https://redsale.by/[^/]+/[^/]+$", item["source"])
        if url_second_level is not None:
            self.run(item["source"])

    def section_list(self):
        self.start_for_list_url(self.all_section)

    # запуск скрипта по времени
    def start(self):

        self.section_list()
        # schedule.every(1).seconds.do(self.start_for_list_url)
        # # schedule.every().day.at("10:30").do(job)
        #
        # while True:
        #     schedule.run_pending()
        #     time.sleep(1)

    # генерация list_links
    def generate_list_link(self):
        list_links = []
        for item in self.all_section:
            url_second_level = re.search(r"https://redsale.by/[^/]+/[^/]+$", item["source"])
            if url_second_level is not None:
                list_links.append(item["source"])

        json_work("other_files/list_links.json", "w", list_links)


if __name__ == "__main__":
    queries = Queries()
    # queries.run("some_url")
    # queries.generate_list_link()
    # queries.section_list()
    queries.run("https://redsale.by/remont/stuccoing/shtukaturka-mayakam")
