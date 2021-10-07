# -*- coding: utf-8 -*- 

import sys, os
import datetime, time
import asyncio
from aiohttp import ClientSession
from pymongo import MongoClient
import pymongo, ssl
import requests
import json

token_name_list : list = os.environ["TOKEN_NAME"].split(" ")
max_length : int = int(os.environ["MAX_LENGTH"])
loop_time : float = float(os.environ["LOOP_TIME"])
data_url : str = os.environ["DATA_URL"] 
cal_loop : int = int(60/loop_time)

mongoDB_connect_info : dict = {
    "host" : os.environ["mongoDB_HOST"],
    "username" : os.environ["USER_ID"],
    "password" : os.environ["USER_PASSWORD"]
    }

#1분
prices_dict_one : dict = {}
prices_candle_dict_one : dict = {"Time": ""}
#5분
prices_dict_five : dict = {}
prices_candle_dict_five : dict = {"Time": ""}
#15분
prices_dict_fifteen : dict = {}
prices_candle_dict_fifteen : dict = {"Time": ""}
#1시간
prices_dict_hour : dict = {}
prices_candle_dict_hour : dict = {"Time": ""}
#4시간
prices_dict_four_hour : dict = {}
prices_candle_dict_four_hour : dict = {"Time": ""}
#1일
prices_dict_day : dict = {}
prices_candle_dict_day : dict = {"Time": ""}

price_db = None
coin_json_data : dict = {}
lp_json_data : dict = {}
    
for k in token_name_list:
    prices_dict_one[k] = []
    prices_candle_dict_one[k] = []
    prices_dict_five[k] = []
    prices_candle_dict_five[k] = []
    prices_dict_fifteen[k] = []
    prices_candle_dict_fifteen[k] = []
    prices_dict_hour[k] = []
    prices_candle_dict_hour[k] = []
    prices_dict_four_hour[k] = []
    prices_candle_dict_four_hour[k] = []
    prices_dict_day[k] = []
    prices_candle_dict_day[k] = []

def get_json():
    try:
        klayswap_info = requests.get(data_url).json()
        return True, klayswap_info
    except Exception as e:
        print(f"{datetime.datetime.now().strftime('%m/%d %H:%M')} : {e}")
        return False, "" 

def save_prices_history(token_info):
    prices = dict()
    prices['Time'] = (datetime.datetime.now() + datetime.timedelta(hours = int(9))).strftime('%m/%d %H:%M')
    for data in token_info:
        if data["symbol"].lower() in token_name_list:
            prices[data["symbol"].lower()] = round(float(data["volume"])/float(data["amount"]), 8)
    return prices

def db_update_prices(db, index : int, input_prices : dict, input_prices_dict : dict, input_prices_candle_dict : dict):
    input_prices_candle_dict["Time"] = input_prices["Time"]
    for k, v in input_prices_candle_dict.items():
        if k != "Time":
            input_prices_dict[k] = [v[0][3]]

    if db.count_documents({}) >= max_length * 1.05:
        delete_db_index = db.find().sort([("_id",1)]).limit(1)
        first_index = list(delete_db_index)[0]["_id"]
        db.find_one_and_delete({"_id":first_index})

    db.update_one({"_id":index}, {"$set" : input_prices_candle_dict}, upsert=True)
        
    return input_prices_dict, input_prices_candle_dict

def main():
    global prices_dict_one
    global prices_candle_dict_one
    global prices_dict_five
    global prices_candle_dict_five
    global prices_dict_fifteen
    global prices_candle_dict_fifteen
    global prices_dict_hour
    global prices_candle_dict_hour
    global prices_dict_four_hour
    global prices_candle_dict_four_hour
    global prices_dict_day
    global prices_candle_dict_day
    global price_db

    one_index : int = 0
    five_index : int = 0
    fifteen_index : int = 0
    hour_index : int = 0
    four_hour_index : int = 0
    day_index : int = 0
    cnt : int = 0
    
    try:
        price_db = MongoClient(ssl=True, ssl_cert_reqs=ssl.CERT_NONE, **mongoDB_connect_info)
        price_db.admin.command("ismaster") # 연결 완료되었는지 체크
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\ndb 연결 완료. 아이디:{mongoDB_connect_info['username']}")
    except pymongo.errors.ServerSelectionTimeoutError:
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\ndb 연결 실패! host 리스트를 확인할 것.")
    except pymongo.errors.OperationFailure:
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\ndb 로그인 실패! username과 password를 확인할 것.")
    except:
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\ndb 연결 실패! 오류 발생:")

    try:
        db_one_index = price_db.coin.price_one.find({}).distinct('_id')
        if len(db_one_index) != 0:
            one_index = max(db_one_index) + 1
    except:
        pass
    try:
        db_five_index = price_db.coin.price_five.find({}).distinct('_id')
        if len(db_five_index) != 0:
            five_index = max(db_five_index) + 1
    except:
        pass
    try:
        db_fifteen_index = price_db.coin.price_fifteen.find({}).distinct('_id')
        if len(db_fifteen_index) != 0:
            fifteen_index = max(db_fifteen_index) + 1
    except:
        pass
    try:
        db_hour_index = price_db.coin.price_hour.find({}).distinct('_id')
        if len(db_hour_index) != 0:
            hour_index = max(db_hour_index) + 1
    except:
        pass
    try:
        db_four_hour_index = price_db.coin.price_four_hour.find({}).distinct('_id')
        if len(db_four_hour_index) != 0:
            four_hour_index = max(db_four_hour_index) + 1
    except:
        pass
    try:
        db_day_index = price_db.coin.price_day.find({}).distinct('_id')
        if len(db_day_index) != 0:
            day_index = max(db_day_index) + 1
    except:
        pass

    while True:
        start = datetime.datetime.now()
        # try:
        if cnt == cal_loop:
            prices_dict_one, prices_candle_dict_one = db_update_prices(price_db.coin.price_one, one_index, prices, prices_dict_one, prices_candle_dict_one)
            one_index += 1
            cnt = 0

            if one_index != 0 and one_index % 5 == 0:
                prices_dict_five, prices_candle_dict_five = db_update_prices(price_db.coin.price_five, five_index, prices, prices_dict_five, prices_candle_dict_five)
                five_index += 1

                if five_index != 0 and five_index % 3 == 0:
                    prices_dict_fifteen, prices_candle_dict_fifteen = db_update_prices(price_db.coin.price_fifteen, fifteen_index, prices, prices_dict_fifteen, prices_candle_dict_fifteen)
                    fifteen_index += 1

                    if fifteen_index != 0 and fifteen_index % 4 == 0:
                        prices_dict_hour, prices_candle_dict_hour = db_update_prices(price_db.coin.price_hour, hour_index, prices, prices_dict_hour, prices_candle_dict_hour)
                        hour_index += 1

                        if hour_index != 0 and hour_index % 4 == 0:
                            prices_dict_four_hour, prices_candle_dict_four_hour = db_update_prices(price_db.coin.price_four_hour, four_hour_index, prices, prices_dict_four_hour, prices_candle_dict_four_hour)
                            four_hour_index += 1

                            if four_hour_index != 0 and four_hour_index % 6 == 0:
                                prices_dict_day, prices_candle_dict_day = db_update_prices(price_db.coin.price_day, day_index, prices, prices_dict_day, prices_candle_dict_day)
                                day_index += 1

        result, toten_data = get_json()

        if not result:
            cnt += 1

            loop_end = (datetime.datetime.now() - start).total_seconds()

            delay_time = loop_time - loop_end
            if delay_time < 0:
                delay_time = 1

            time.sleep(delay_time)
            continue
        
        try:
            prices = save_prices_history(toten_data["tokenInfo"])
        except:
            cnt += 1

            loop_end = (datetime.datetime.now() - start).total_seconds()

            delay_time = loop_time - loop_end
            if delay_time < 0:
                delay_time = 1

            time.sleep(delay_time)
            continue

        for k in prices_dict_one.keys():
            prices_dict_one[k].append(prices[k])

        if not prices_candle_dict_one["Time"]:
            prices_candle_dict_one["Time"] = prices["Time"]
            prices_candle_dict_five["Time"] = prices["Time"]
            prices_candle_dict_fifteen["Time"] = prices["Time"]
            prices_candle_dict_hour["Time"] = prices["Time"]
            prices_candle_dict_four_hour["Time"] = prices["Time"]
            prices_candle_dict_day["Time"] = prices["Time"]

            for k, v in prices_dict_one.items():
                prices_candle_dict_one[k].append([v[0], max(v), min(v), v[len(v)-1]])
                prices_dict_five[k] = prices_candle_dict_one[k][0]
                prices_dict_fifteen[k] = prices_candle_dict_one[k][0]
                prices_dict_hour[k] = prices_candle_dict_one[k][0]
                prices_dict_four_hour[k] = prices_candle_dict_one[k][0]
                prices_dict_day[k] = prices_candle_dict_one[k][0]
        else:
            prices_candle_dict_one["Time"] = prices["Time"]
            for k, v in prices_dict_one.items():
                prices_candle_dict_one[k][len(prices_candle_dict_one[k])-1] = [v[0], max(v), min(v), v[len(v)-1]]

        if not prices_dict_five["klay"]:
            prices_candle_dict_five["Time"] = prices["Time"]
            prices_candle_dict_fifteen["Time"] = prices["Time"]
            prices_candle_dict_hour["Time"] = prices["Time"]
            prices_candle_dict_four_hour["Time"] = prices["Time"]
            prices_candle_dict_day["Time"] = prices["Time"]
            for k in prices_dict_one.keys():    
                prices_dict_five[k] = prices_candle_dict_one[k][0]
                prices_dict_fifteen[k] = prices_candle_dict_one[k][0]
                prices_dict_hour[k] = prices_candle_dict_one[k][0]
                prices_dict_four_hour[k] = prices_candle_dict_one[k][0]
                prices_dict_day[k] = prices_candle_dict_one[k][0]

        for k in prices_dict_one.keys():
            tmp_five = prices_dict_five[k] + prices_candle_dict_one[k][0]
            result_five = [tmp_five[0], max(tmp_five), min(tmp_five), tmp_five[len(tmp_five)-1]]
            prices_candle_dict_five[k] = [result_five]
            prices_dict_five[k] = result_five

            tmp_fifteen = prices_dict_fifteen[k] + prices_candle_dict_one[k][0]
            result_fifteen = [tmp_fifteen[0], max(tmp_fifteen), min(tmp_fifteen), tmp_fifteen[len(tmp_fifteen)-1]]
            prices_candle_dict_fifteen[k] = [result_fifteen]
            prices_dict_fifteen[k] = result_fifteen

            tmp_hour = prices_dict_hour[k] + prices_candle_dict_one[k][0]
            result_hour = [tmp_hour[0], max(tmp_hour), min(tmp_hour), tmp_hour[len(tmp_hour)-1]]
            prices_candle_dict_hour[k] = [result_hour]
            prices_dict_hour[k] = result_hour

            tmp_four_hour = prices_dict_four_hour[k] + prices_candle_dict_one[k][0]
            result_four_hour = [tmp_four_hour[0], max(tmp_four_hour), min(tmp_four_hour), tmp_four_hour[len(tmp_four_hour)-1]]
            prices_candle_dict_four_hour[k] = [result_four_hour]
            prices_dict_four_hour[k] = result_four_hour

            tmp_day = prices_dict_day[k] + prices_candle_dict_one[k][0]
            result_day = [tmp_day[0], max(tmp_day), min(tmp_day), tmp_day[len(tmp_day)-1]]
            prices_candle_dict_day[k] = [result_day]
            prices_dict_day[k] = result_day

        price_db.coin.price_one.update_one({"_id":one_index}, {"$set" : prices_candle_dict_one}, upsert=True)
        price_db.coin.price_five.update_one({"_id":five_index}, {"$set" : prices_candle_dict_five}, upsert=True)
        price_db.coin.price_fifteen.update_one({"_id":fifteen_index}, {"$set" : prices_candle_dict_fifteen}, upsert=True)
        price_db.coin.price_hour.update_one({"_id":hour_index}, {"$set" : prices_candle_dict_hour}, upsert=True)
        price_db.coin.price_four_hour.update_one({"_id":four_hour_index}, {"$set" : prices_candle_dict_four_hour}, upsert=True)
        price_db.coin.price_day.update_one({"_id":day_index}, {"$set" : prices_candle_dict_day}, upsert=True)
        
        cnt += 1

        loop_end = (datetime.datetime.now() - start).total_seconds()

        delay_time = loop_time - loop_end
        if delay_time < 0:
            delay_time = 1

        time.sleep(delay_time)

if __name__ == "__main__":
    main()
    #
