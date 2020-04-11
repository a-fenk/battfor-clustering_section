import json
import re
import string

import requests
from nltk.stem.snowball import SnowballStemmer

from all_constants import STOPWORDS, API_WORDSTAT, TOKEN_YM, EXCEPT_URLS

''' Вспомогательные функции '''


def tag_to_string(tags):
    strings = []
    for tag in tags:
        strings.append(tag.string)
    return strings


def stemmed(words):
    stemmer = SnowballStemmer("russian")
    list_words = words.split()
    list_out_words = []
    for word in list_words:
        list_out_words.append(stemmer.stem(word))

    return " ".join(list_out_words)


def change_mask(text):
    text = text.lower()
    text = re.sub('[%s]' % re.escape(string.punctuation), '', text)  # Удаление пунктуации
    tmp = text.split(" ")
    stem_text = stemmed(text)
    word_list = stem_text.split(" ")
    for word in STOPWORDS:
        word = stemmed(word)
        for word_ in word_list:
            if word == word_:
                tmp.pop(word_list.index(word_))

    return " ".join(tmp)


def check_except_url(url):
    status = True
    for i in EXCEPT_URLS:
        if i == url:
            status = False

    return status


def check_geo(text):
    list_ = []
    status = True
    with open("other_files/geo.txt", "r") as f:
        for i in f:
            list_.append(i.strip().lower())
    for word in list_:
        if word in text:
            status = False

    return status


def masked(text):
    maska = {}
    text = re.sub(r"\(\w+\)", "", text)
    text = change_mask(text).strip()
    with_minsk = text.replace('в минске', 'минск')
    if with_minsk.count("минск") < 1 and check_geo(with_minsk):
        with_minsk += ' минск'
    elif with_minsk.count("минск") > 1:
        with_minsk = with_minsk.replace('минск', '')
    without_minsk = with_minsk.replace('минск', '')
    maska["with_minsk"] = with_minsk
    maska["without_minsk"] = without_minsk
    return maska


def split_list(list_in, delimiter="/"):
    list_ = []
    tmp = []
    for i in list_in:
        if i != delimiter:
            list_.append(i)
        else:
            if len(list_) > 0:
                tmp.append(list_)
            list_ = []
    return tmp


def json_work(filename, method, write_data=None):
    if write_data is None:
        write_data = []

    with open(filename, method, encoding="utf-8") as f:
        if method == "r":
            data = json.load(f)
            return data
        elif method == "w":
            json.dump(write_data, f, ensure_ascii=False, indent=2)


def get_request_to_ya(data):
    jdata = json.dumps(data, ensure_ascii=False).encode('utf-8')
    try:
        response = requests.post(API_WORDSTAT, jdata, headers={"Authorization": f"Bearer {TOKEN_YM}"})
        resp_json = response.json()
        return resp_json
    except requests.exceptions.ConnectionError:
        return get_request_to_ya(data)
