import multiprocessing
import re
import sys
import time
from datetime import datetime, timedelta

import pandas as pd

from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup
from lxml import etree

from all_constants import TOKEN_YM, SITE_MAP, API_XMLRIVER
from helping_functions import tag_to_string, masked, stemmed, json_work, get_request_to_ya, \
    check_except_url

''' Основная логика'''

# def init(l):
#     global lock

lock = multiprocessing.Lock()


class AllSection:
    def __init__(self, sitemap=None):
        self.sitemap = sitemap
        self.list_url = pd.Series()
        self.all_section = json_work("other_files/all_section.json", 'r')
        self.count_river = 0
        self.trying_freq = 0
        self.count = 0

    # удаление отчета аналитики
    def delete_wordstat_report(self, id_):
        data = {
            "method": "DeleteForecastReport",
            "param": id_
        }

        get_request_to_ya(data)

    # получение отчета аналитики
    def get_wordstat_report(self, id_):
        data = {
            "method": "GetForecast",
            "param": id_,
            "token": TOKEN_YM,
            "locale": "ru",
        }
        resp_json = get_request_to_ya(data)

        try:
            data = resp_json["data"]
            self.delete_wordstat_report(id_)
            out = data["Phrases"]
            # print(out)
            return out
        except (KeyError, TypeError):
            print(f"{resp_json}")
            time.sleep(5)
            return self.get_wordstat_report(id_)

    # создание отчета аналитики из wordstat
    def create_wordstat_analytics(self, phrase):
        data = {
            "method": "CreateNewForecast",
            "startDate": "2020-02-30",
            "endDate": "2020-04-03",
            "param": {
                "Phrases":
                    phrase
                ,
                "GeoID": [
                    149
                ],
                "Currency": "RUB",
            },
            "token": TOKEN_YM,
            "locale": "ru",
        }
        resp_json = get_request_to_ya(data)

        try:
            id_ = resp_json["data"]
            print("Отчёт создан")
            return id_
        except KeyError:
            print(f"Ошибка: {resp_json}")
            if resp_json["error_code"] == 71:
                print("В запросе больше 7 символов, возвращаю -1")
                return -1
            return None

    def get_sources(self):
        url = []
        for item in self.all_section:
            url.append(item["source"])
        return pd.Series(url)

    def get_frequency(self, phrase):
        id_ = self.create_wordstat_analytics(phrase)
        # TODO выловить ошибку для None
        if id_ == -1:
            er = [0]["shown"]
            er[0]["shown"] = 0
            return er
        if id_ is None:
            if self.trying_freq < 3:
                self.trying_freq += 1
                return self.get_frequency(phrase)
            else:
                freq = 0
                self.trying_freq = 0
                return freq

        freq = self.get_wordstat_report(id_)
        self.trying_freq = 0

        return freq

    # Получение ссылок из карты сайта
    def get_sitemap(self, url):
        list_url = []
        response = requests.get(url)
        tree = etree.fromstring(response.content)

        for sitemap in tree:
            children = sitemap.getchildren()
            url = children[0].text
            section = re.search(r'https://redsale.by/sections\S*', url)
            if section is None and check_except_url(url):
                list_url.append(url)
        return pd.Series(list_url)

    def check_freq(self):
        if len(self.all_section) >= 100:
            self.create_request_frequency()

    # Получение заголовков из ссылок карты сайта
    def get_h1_from_url(self, url):
        list_site = []
        print(f"Обработка {url}")
        tmp = {}
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'lxml')
        h1 = soup.h1.text.strip()
        tmp["source"] = url
        tmp["h1"] = h1
        self.create_out_data(tmp)

    def get_xml_river(self, id_, text):
        SERP = {}
        response = requests.get(f"{API_XMLRIVER}&req_id={id_}")
        soup = BeautifulSoup(response.content, 'lxml')
        try:
            status = tag_to_string(soup.find("status"))[0]
        except TypeError:
            status = "200"
            try:
                group = soup.find("results")
                urls = group.find_all_next("url")
                titles = group.find_all_next("title")
                descriptions = group.find_all_next("passage")

                SERP["url"] = tag_to_string(urls)
                SERP["title"] = tag_to_string(titles)
                SERP["description"] = tag_to_string(descriptions)
                print(f"SERP по {text} получен")

            except TypeError:
                self.get_xml_river(id_, text)
            except AttributeError:
                print(f"XMLRiver не смог получить данные по {text}")
                return -1


        if status == "WAIT":
            time.sleep(2)
            # print(multiprocessing.current_process())
            print(f"Ждём SERP по {text}")
            return self.get_xml_river(id_, text)

        elif status == "ERROR Bad request id":
            SERP["url"] = []
            SERP["title"] = []
            SERP["description"] = []
            print(f"SERP по {text} пуст")

        return SERP

    # Формирование SERP из xmlriver
    def xml_river(self, text):
        response = requests.get(f"{API_XMLRIVER}&query={text}&delayed=1")
        soup = BeautifulSoup(response.content, 'lxml')
        id_ = soup.find("req_id")
        id_ = tag_to_string(id_)[0]
        SERP = self.get_xml_river(id_, text)
        if SERP == -1:
            print("serp =", SERP)
            return -1
        return SERP

    def create_serp_from_arsenkin(self, text):
        pass

    def add_frequency_to_all(self, list_phrase):
        for item in self.all_section:
            for item1 in list_phrase:
                if f'{item["maska"]["without_minsk"]} цена' == item1["Phrase"] or \
                        item["maska"]["with_minsk"] == item1["Phrase"]:
                    item["frequency"]["basic"] += item1["Shows"]
                elif f'"{item["maska"]["without_minsk"]} цена"' == item1["Phrase"] or \
                        f'"{item["maska"]["with_minsk"]}"' == item1["Phrase"]:
                    item["frequency"]["accurate"] += item1["Shows"]

        json_work("other_files/all_section.json", "w", self.all_section)

    def create_request_frequency(self):
        tmp_list = []
        for item in self.all_section:
            if len(tmp_list) >= 100:
                list_freq = self.get_frequency(tmp_list)
                self.add_frequency_to_all(list_freq)
                tmp_list = []

            list_ = [f'{item["maska"]["without_minsk"]} цена',
                     item["maska"]["with_minsk"],
                     f'"{item["maska"]["with_minsk"]}"',
                     f'"{item["maska"]["without_minsk"]} цена"'
                     ]

            tmp_list.extend(list_)

    # Генерация template
    def generate_template(self, template):
        template_for_sections = {}
        frequency = {}
        h1 = template["h1"]
        maska = masked(h1)
        stemming = stemmed(maska["without_minsk"])
        SERP = self.xml_river(maska["with_minsk"])
        if SERP == -1:
            return
        # TODO сделать по 100 фраз в массиве
        basic_freq = self.get_frequency([maska["with_minsk"]])[0]["Shows"]
        basic_freq += self.get_frequency([f'{maska["without_minsk"]} цена'])[0]["Shows"]
        accurate_freq = self.get_frequency([f'"{maska["with_minsk"]}"'])[0]["Shows"]
        accurate_freq += self.get_frequency([f'"{maska["without_minsk"]} цена"'])[0]["Shows"]

        frequency["basic"] = basic_freq
        frequency["accurate"] = accurate_freq
        template_for_sections["h1"] = template["h1"]
        template_for_sections["maska"] = maska
        template_for_sections["stemming"] = stemming
        template_for_sections["source"] = template["source"]
        template_for_sections["SERP"] = SERP
        template_for_sections["heading_entry"] = self.get_heading(SERP, stemming)
        template_for_sections["frequency"] = frequency

        return template_for_sections

    # Получение количества вхождений
    def get_heading(self, serp, stemming):
        list_title_in_serp = []
        heading_entry = 0

        try:
            list_title_in_serp = serp["title"]
        except KeyError:
            pass
            # print(serp)

        # print(f'Все title из serp - {list_title_in_serp}')
        list_title_in_maska = stemming.split(' ')
        # print(f'Стимминг - {list_title_in_maska}')
        for title in list_title_in_serp:
            tmp = 0
            for title_ in list_title_in_maska:
                # print(f'подстрока {title_}, встречается в {title} {title.lower().count(title_)}раз')
                if title.lower().count(title_) > 0:
                    tmp += 1
                if tmp >= len(list_title_in_maska):
                    heading_entry += 1
        # print(f'Количество вхождений: {heading_entry}')

        return heading_entry

    # Проверка на присутствие в all_section
    @staticmethod
    def check_in_allsection(data1, all_section):
        url_list = [url["source"] for url in all_section]

        for i in data1:
            if i["source"] in url_list:
                return False
            else:
                return True

    # Формирование allsection.json
    def create_out_data(self, template):
        out_data = []
        # data = list_site
        print(f'Template по {template} обработан')
        data_from_template = [self.generate_template(template)]
        if data_from_template:
            data_in_json = json_work("other_files/all_section.json", "r")
                # if self.check_in_allsection(data_from_template, data_in_json):
            general_data = data_in_json + data_from_template
            json_work("other_files/all_section.json", "w", general_data)

            print(f'url {template["source"]} добавлен в json')
            self.count += 1
            print(f'Всего url добавлено за сессию: {self.count}')
        return

    def delete_duplicates(self, ser_from, ser_rm):
        ser_from = pd.Series(ser_from)
        ser_rm = pd.Series(ser_rm)
        ser_from = ser_from.append(ser_rm.append(ser_rm))
        ser_from = ser_from.drop_duplicates(keep=False)
        ser_from = ser_from.reset_index(drop=True)
        return ser_from

    def check_sitemap(self):
        sources_json = self.get_sources()
        print(f"Всего URL в sitemap: {self.list_url.size}")
        print(f"Всего URL в all_section.json: {sources_json.size}")
        url_to_delete = self.delete_duplicates(sources_json, self.list_url)  # Получаем серию URL которую нужно удалить
        url_to_add = self.delete_duplicates(self.list_url, sources_json)  # Получаем серию URL которую добавить
        for idx, item in enumerate(self.all_section):
            if (url_to_delete.isin([item['source']])).any():
                print(f"url {item['source']} был удален из json")
                self.all_section.pop(idx)  # удаление элементов, отсутствующих в sitemap
        json_work("other_files/all_section.json", "w", self.all_section)
        # for url in url_to_add:  # здесь должны включаться потоки
        with ThreadPoolExecutor(5) as executor:
            for _ in executor.map(self.get_h1_from_url, url_to_add):
                pass
        # self.get_h1_from_url(url)

    # обновление serp sitemap
    def update_serp(self):
        for item in self.all_section:
            frequency = {}

            basic_freq = self.get_frequency([item["maska"]["with_minsk"]])[0]["Shows"]
            basic_freq += self.get_frequency([f'{item["maska"]["without_minsk"]} цена'])[0]["Shows"]
            accurate_freq = self.get_frequency([f'"{item["maska"]["with_minsk"]} "'])[0]["Shows"]
            accurate_freq += self.get_frequency([f'"{item["maska"]["without_minsk"]} цена"'])[0]["Shows"]

            frequency["basic"] = basic_freq
            frequency["accurate"] = accurate_freq
            item["frequency"] = frequency

        json_work("other_files/all_section.json", "w", self.all_section)

    # Порядок запуска функций
    def run(self, update=False):
        self.list_url = self.get_sitemap(self.sitemap)
        if update:  # Если в командной строке есть update то обновляем all_section
            self.check_sitemap()
            # self.update_serp() # Полное обновление информации
        else:
            self.get_h1_from_url(self.list_url)
            # l = Lock()
            # p = Pool(initializer=init, initargs=(lock,), processes=5)
            # p.map(self.get_h1_from_url, self.list_url)
            # p.join()
            # p.close()


if __name__ == "__main__":
    try:
        update = sys.argv[1]
    except IndexError:
        update = ""
    all_section = AllSection(SITE_MAP)
    if update == "update":
        all_section.run(update=True)

    else:
        all_section.run()
