# coding: utf-8

import os
import glob
import sys
from datetime import datetime, timedelta
from urllib import quote
from bson import json_util
from pymongo import MongoClient

MONGODB_HOST_PORT = "公网地址:27017"
MONGODB_PASSWORD = ""


def get_mongodb_database(database, user="third"):
    url = "mongodb://{0}:{1}@{2}/{3}".format(
        user, quote(MONGODB_PASSWORD), MONGODB_HOST_PORT, database
    )
    client = MongoClient(host=url, maxPoolSize=1, minPoolSize=1)
    return client.get_default_database()


def recovery(db, names):
    files = glob.glob("*.bak.json")
    for file in files:
        name = file.split(".")[1]
        assert name in names
        data = open(file).read()
        docs = json_util.loads(data)
        col = db[name]
        try:
            result = col.insert_many(docs)
            print "Insert %s count %s" % (name, len(result.inserted_ids))
        except Exception as e:
            print e


def backup(db, names):
    now = datetime.now().strftime("%Y-%m-%d.")
    yestoday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d.")
    for name in names:
        print "start backup %s" % name
        key = now + name + ".bak.json"
        key_yestoday = yestoday + name + ".bak.json"
        col = db[name]
        docs = list(col.find())
        data = json_util.dumps(docs)
        with open(key, "wb") as f:
            f.write(data)
        try:
            print "try remove", key_yestoday
            os.remove(key_yestoday)
        except Exception as e:
            print e


def main():
    names = ["company", "data_category_one", "data_category_two", "data_forms", "qidian_map",
             "source_status", "spider_advertisements", "spider_channels", "spider_configs",
             "spider_sites", "timerules", "region_map", "weibo_account"]
    thirdparty = get_mongodb_database("thirdparty", "third")

    action = sys.argv[1]

    if action.strip() == "b":
        backup(thirdparty, names)
    elif action.strip() == "r":
        recovery(thirdparty, names)
    else:
        print "Action not vaild"


if __name__ == "__main__":
    main()
