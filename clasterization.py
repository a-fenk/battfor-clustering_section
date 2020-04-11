import os
from pprint import pprint

import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.workbook.workbook import Workbook

from helping_functions import json_work, split_list

data = json_work("other_files/main.json", "r")


class Clasterization:
    def __init__(self, work_file, sheet_name="sheet", name_doc="file_"):
        self.work_file = work_file
        self.list_query = []
        self.name_sheet = sheet_name
        self.name_doc = name_doc
        # self.url = url

    # Получаем список запросов
    def get_dict_from_work(self):
        for item in self.work_file:
            self.list_query.append(item["maska"]["with_minsk"])

    def set_claster(self, list_in, lvl):
        tmp_list = []  # входящие в кластер
        for query in list_in:
            query_urls = self.get_data(query)["SERP"]["url"]
            for query1 in list_in:
                query_urls1 = self.get_data(query1)["SERP"]["url"]

                if len(set(query_urls) & set(query_urls1)) >= 7 and query1 not in tmp_list:  # если 7+ общих
                    tmp_list.append(query1)  # добавляем в кластер

            tmp_list.append("/")  # отделение кластера
            # print(tmp_list)
        return tmp_list

    # def hard_claster(self, list_):
    #     tmp_list = []
    #     for list_item in list_:
    #         if len(list_item) >= 2:
    #             tmp_list.append(list_item[0])
    #             tmp_list_1 = self.set_claster(list_item[1:])
    #             tmp = split_list(tmp_list_1)
    #             tmp_list.append(self.hard_claster(tmp))
    #         else:
    #             tmp_list.append(list_item)
    #
    #     # pprint(tmp_list)
    #     return tmp_list

    def get_match(self, list_):
        # tmp = self.hard_claster(tmp)
        # print(tmp)
        out_list = []

        for list_lev2 in list_:
            tmp_d = {}
            if len(list_lev2) > 1:
                tmp_d["query"] = list_lev2[0]
                tmp_d["list_query"] = list_lev2[1:]
            else:
                tmp_d["query"] = list_lev2[0]
                tmp_d["list_query"] = []
            out_list.append(tmp_d)

        # print(out_list)

        return out_list

    # Очищаем список от пустых и не пересекающихся запросов
    # TODO сейчас чистит просто от 1ого элемента
    def clean_query(self, query_list):
        tmp = []
        for item in query_list:
            tmp.append(item)

        data_ = self.generate_out_data(tmp)

        return data_

    # Получение frequency из work_file
    def get_data(self, item):
        for i in self.work_file:
            if item == i["maska"]["with_minsk"]:
                return i

    # Генерация данных в виде кластера
    def generate_out_data(self, cluster):
        tmp = []
        for item in cluster:
            tmp_dict = {"cluster": item["query"],
                        "frequency_basic": int(self.get_data(item["query"])["frequency"]["basic"]),
                        "frequency_accurate": int(self.get_data(item["query"])["frequency"]["accurate"]),
                        "heading_entry": int(self.get_data(item["query"])["heading_entry"]),
                        "queries": []}
            try:
                tmp_dict["H1"] = self.get_data(item["query"])["H1_"]
            except KeyError:
                tmp_dict["H1"] = ""
            for query in item["list_query"]:
                try:
                    in_dict = {"query": query,
                               "frequency_basic": int(self.get_data(query)["frequency"]["basic"]),
                               "frequency_accurate": int(self.get_data(query)["frequency"]["accurate"]),
                               "heading_entry": int(self.get_data(query)["heading_entry"])}
                    try:
                        in_dict["H1"] = self.get_data(query)["H1_"]
                    except KeyError:
                        in_dict["H1"] = " "
                    tmp_dict["queries"].append(in_dict)
                except TypeError:
                    pass
            tmp.append(tmp_dict)
            # pprint(tmp)
        return tmp

    # сравнивает urls с all_section и при 7+ общих присваивает h1
    def compare_with(self):
        all_section = json_work("other_files/all_section.json", "r")
        for item in self.work_file:
            for item_ in all_section:
                if len(set(item["SERP"]["url"]) & set(item_["SERP"]["url"])) >= 7:
                    item["H1_"] = item_["h1"]

    # создание exel файла
    def create_excel(self, data_):
        num = 0

        try:
            self.name_doc = os.listdir("excel_files")[-1].split(".")[0]
            workbook = load_workbook(filename=f"excel_files/{self.name_doc}.xlsx")
        except:
            workbook = Workbook(iso_dates=True)

        # print(workbook.worksheets)
        if len(workbook.worksheets) > 10:
            del workbook["Sheet"]
            while os.path.exists(f"excel_files/{self.name_doc}.xlsx"):
                num += 1
                name = self.name_doc.split("_")[0]
                self.name_doc = f"{name}_{num}"
            else:
                workbook = Workbook(iso_dates=True)

        sheet = workbook.create_sheet()

        const = 2
        # sheet["A2"].style = "Good"
        # sheet["B2"].style = "Good"
        # sheet["C2"].style = "Good"
        # sheet["D2"].style = "Good"
        # sheet["F2"].style = "Good"
        sheet["A2"] = "cluster"
        sheet["B2"] = "query"
        sheet[f"{chr(ord('C') + const)}2"] = "frequency_basic"
        sheet[f"{chr(ord('D') + const)}2"] = "freq_accurate"
        sheet[f"{chr(ord('E') + const)}2"] = "heading_entry"
        sheet[f"{chr(ord('F') + const)}2"] = "H1"
        sheet.merge_cells("A1:F1")
        # sheet["A1"] = self.url
        header = sheet["A1"]
        header.style = "Note"
        idx = 3
        pred_cluster = None
        for i in data_:
            idx_ = 0
            if i["cluster"] != pred_cluster:
                sheet[f"{chr(ord('A') + idx_)}{idx}"] = i["cluster"]
                sheet[f"{chr(ord('C') + const)}{idx}"] = i["frequency_basic"]
                sheet[f"{chr(ord('D') + const)}{idx}"] = i["frequency_accurate"]
                sheet[f"{chr(ord('E') + const)}{idx}"] = i["heading_entry"]
                sheet[f"{chr(ord('F') + const)}{idx}"] = i["H1"]
                idx += 1
                idx_ += 1
                try:
                    for q in i["queries"]:
                        sheet[f"{chr(ord('A') + idx_)}{idx}"] = q["query"]
                        sheet[f"{chr(ord('C') + const)}{idx}"] = q["frequency_basic"]
                        sheet[f"{chr(ord('D') + const)}{idx}"] = q["frequency_accurate"]
                        sheet[f"{chr(ord('E') + const)}{idx}"] = q["heading_entry"]
                        sheet[f"{chr(ord('F') + const)}{idx}"] = q["H1"]
                        idx += 1
                    pred_cluster = i['cluster']
                except IndexError:
                    pred_cluster = i['cluster']
                    idx += 1
        workbook.save(filename=f"excel_files/{self.name_doc}.xlsx")
        print(f"{sheet.title} добавлен в {self.name_doc}.xlsx")

    def hard_cluster(self, list_):
        tmp_list = []
        for query in list_:
            if query["queries"]:
                tmp_list += query["queries"]
                tmp_list += '/'
        tmp_list = split_list(tmp_list)
        for idx, item in enumerate(tmp_list):
            query_list = []
            for idx_, query in enumerate(item):
                query_list.append(query["query"])



    # запуск скрипта
    def run(self):
        self.compare_with()
        self.get_dict_from_work()   # получаем список запросов
        lvl = 0
        tmp_list = self.set_claster(self.list_query, lvl)
        lvl += 1
        tmp_list = split_list(tmp_list)
        clusters = []
        for item in tmp_list:
            tmp = self.set_claster(item, lvl)
            tmp = split_list(tmp)
            clusters.append(tmp)
        query_list = self.get_match(tmp_list)
        clean_query_list = self.clean_query(query_list)
        print(query_list)
        if len(clean_query_list) > 0:
            try:
                self.create_excel(clean_query_list)
            except FileNotFoundError:
                os.mkdir("excel_files")
                self.create_excel(clean_query_list)


if __name__ == "__main__":
    claster = Clasterization(data)
    claster.run()
