import re
import sys
import time
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime

import requests

from googleapiclient.discovery import build
from googleapiclient.http import build_http
from oauth2client import client, tools, file
from threading import Lock

from all_constants import SCOPE_SEARCH, CLIENT_SECRETS, TOKEN_YM
from all_section import AllSection
from clustering import Clustering
from helping_functions import masked, stemmed, json_work

all_section = AllSection()
get_serp = all_section.xml_river
get_heading = all_section.get_heading
get_frequency = all_section.get_frequency
url_ = ["https://redsale.by/remont/poshiv-odezhdy/poshiv-bryuk",
        "https://redsale.by/remont/poshiv-odezhdy/poshiv-bryuk"]


lock = Lock()


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
            "startDate": "2018-01-01",
            "endDate": datetime.today().strftime('%Y-%m-%d'),
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
                print("ошибкa!")
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
            if self.checkin_stemming(list_tmp, tmp_queries):    # проверка на совпадение раннее
                if self.checkin_stemming(self.all_section, tmp_queries):    # проверка на совпадение в all_section
                    if self.checkin_stemming(self.main_file, tmp_queries):  # проверка на совпадение в main_file
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
        stemming = item["stemming"]
        try:
            basic_freq = get_frequency([maska["with_minsk"]])[0]["Shows"]
            basic_freq += get_frequency([f'{maska["without_minsk"]} цена'])[0]["Shows"]
            if basic_freq == 0:
                frequency["basic"] = 0
                frequency["accurate"] = 0
                item["frequency"] = frequency
                lock.acquire()
                main = json_work("other_files/main.json", "r")
                main.append(item)
                json_work("other_files/main.json", "w", main)
                print("basic freq = 0")
                print(f'Запрос {maska["with_minsk"]} был добавлен в main.json')
                lock.release()
                return
            accurate_freq = get_frequency([f'"{maska["with_minsk"]}"'])[0]["Shows"]
            accurate_freq += get_frequency([f'"{maska["without_minsk"]} цена"'])[0]["Shows"]
        except TypeError:
            frequency["basic"] = 0
            frequency["accurate"] = 0
            item["frequency"] = frequency
            lock.acquire()
            main = json_work("other_files/main.json", "r")
            main.append(item)
            json_work("other_files/main.json", "w", main)
            print(f'Запрос {maska["with_minsk"]} был добавлен в main.json')
            lock.release()
            return

        serp = get_serp(maska["with_minsk"])
        if serp == -1:
            print(f'Отменяем дальнейшую работу с запросом {maska["with_minsk"]}')
            return

        frequency["basic"] = basic_freq
        frequency["accurate"] = accurate_freq
        item["SERP"] = serp
        item["heading_entry"] = get_heading(serp, stemming)
        item["frequency"] = frequency

        lock.acquire()
        work = json_work("other_files/work_file.json", "r")
        work.append(item)
        json_work("other_files/work_file.json", "w", work)
        print(f'Запрос {maska["with_minsk"]} был добавлен в work_file.json')
        lock.release()

    def checkin_main(self, main_file, keys):
        for item in main_file:
            if item["query"] in keys:
                # print(f'запрос {item["query"]} был удален из ключей')
                keys.remove(item["query"])

    # получение ключей из текстового файла
    def get_key_from_txt(self):
        list_keys = []
        with open("other_files/keys.txt") as f:
            for item in f:
                if item.strip().lower() not in list_keys:
                    list_keys.append(item.strip().lower())

        return list_keys

    # получение ссылок из текстового файла
    def get_links_from_txt(self):
        list_links = []
        with open("other_files/links.txt") as f:
            for item in f:
                list_links.append(item.rstrip('\n'))

        return list_links

    # проверка наличия stemming вне зависимости от последовательности слов
    def checkin_stemming(self, stemmings, item):
        for item_ in stemmings:
            if set(item["stemming"].split(' ')) == set(item_["stemming"].split(' ')):
                # print(f'{item["stemming"]} = {item_["stemming"]}')
                return False
        return True

    def generate(self, keys, url):
        json_work("other_files/work_file.json", "w", [])    # обнуляем work

        print(f'Ключей получено: {len(keys)}')

        if len(keys) > 0:
            self.generate_pretmp(keys)  # генерация претемплейтов по ключам c уникальным stemming
            print(f'Ключей после удаления дублей: {len(self.work_file)}')
            time.sleep(2)
            if len(self.work_file) > 0:
                with ThreadPoolExecutor(5) as executor:
                    for _ in executor.map(self.template_generated, self.work_file):
                        pass
                work = json_work("other_files/work_file.json", "r")
                if len(work) > 0:
                    gen_data = sorted(work, key=lambda x: x["frequency"]["basic"], reverse=True)
                    json_work("other_files/work_file.json", "w", gen_data)
                    gen_data += json_work("other_files/main.json", "r")
                    gen_data = sorted(gen_data, key=lambda x: x["frequency"]["basic"], reverse=True)
                    json_work("other_files/main.json", "w", gen_data)
                    print(f"url {url} обработан")
                    clustering = Clustering(json_work("other_files/work_file.json", "r"), url)
                    clustering.run()
            else:
                print("Перехожу к следующему url")
        return

    # Запуск скрипта
    def run(self, manual_keys=False, manual_links=False):
        # all_section.delete_all_reports()

        keys = []
        # self.get_from_ym(url)
        if manual_keys:
            keys += self.get_key_from_txt()  # получение ключей из файла
            self.generate(keys, "None")
        elif manual_links:
            urls = self.get_links_from_txt()
            # print(urls)
            for url in urls:
                print(f"Получаю ключи по {url} ...")
                keys = self.get_keys_from_gls(url)  # получение ключей gsc
                if keys:
                    self.generate(keys, url)
                else:
                    print("Список ключей пуст.")

        else:
            if not json_work("other_files/list_links.json", "r"):  # если список пустой, наполняем из all_section
                print("list_link.json пуст, получаю URL из all_section.json ...")
                self.generate_list_link()
            list_links = json_work("other_files/list_links.json", "r")
            urls = self.get_urls_with_limit(list_links, 5)  # берем первые пять эл-ов
            print(urls)
            for url in urls:
                print(f"Получаю ключи по {url} ...")
                keys = self.get_keys_from_gls(url)  # получение ключей gsc
                if keys:
                    self.generate(keys, url)
                    list_links.remove(url)
                    print(f"url {url} был обработан и удален из list_links.json")
                    print(f"Всего элементов осталось в list_links.json: {len(list_links)}")
                    json_work("other_files/list_links.json", "w", list_links)
                else:
                    print("Список ключей пуст.")

        print("Завершено")

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

    # запуск скрипта автоматом
    def get_all_section_url(self):

        url_second_level = re.search(r"https://redsale.by/[^/]+/[^/]+$", item["source"])
        if url_second_level is None:
            return None

        return

    # запуск скрипта по времени
    def start(self):

        self.section_list()
        # schedule.every(1).seconds.do(self.start_for_list_url)
        # # schedule.every().day.at("10:30").do(job)
        #
        # while True:
        #     schedule.run_pending()
        #     time.sleep(1)

    # генерация list_links из all section
    def generate_list_link(self):
        list_links = []
        for item in self.all_section:
            url_second_level = re.search(r"https://redsale.by/[^/]+/[^/]+$", item["source"])
            if url_second_level is not None:
                list_links.append(item["source"])

        json_work("other_files/list_links.json", "w", list_links)


    def get_urls_with_limit(self, list_in, limit):
        list_out = []
        print(f'размер list_links.json: {len(list_in)}')
        print("На обработку:")
        for url in list_in[0:limit]:
            list_out.append(url)
            # list_in.pop(0)
            print(url)

        print(f'размер списка на обработку: {len(list_out)}')
        return list_out


if __name__ == "__main__":
    try:
        mode = sys.argv[1]
    except IndexError:
        mode = ""
    queries = Queries()
    if mode == "manual_keys":
        queries.run(manual_keys=True)

    elif mode == "manual_links":
        queries.run(manual_links=True)

    else:
        queries.run()
