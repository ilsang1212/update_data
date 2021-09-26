# -*- coding: utf-8 -*- 

import sys, os
import datetime, time
import asyncio
from aiohttp import ClientSession
from pymongo import MongoClient
import pymongo, ssl
import json

token_name_list : list = os.environ["TOKEN_NAME"].split(" ")
token_hash_list : list = os.environ["TOKEN_HASH"].split(" ")
max_length : int = int(os.environ["MAX_LENGTH"])
loop_time : float = float(os.environ["LOOP_TIME"])
klay_usdt_lp = os.environ["KLAY_USDT_LP"]  # 이건 유지
ks_check_list : list = os.environ["KS_CHECK_LIST"].split(" ")  # 이건 유지
cal_loop : int = int(300/loop_time)

mongoDB_connect_info : dict = {
    "host" : os.environ["mongoDB_HOST"],
    "username" : os.environ["USER_ID"],
    "password" : os.environ["USER_PASSWORD"]
    }

url_list : list = []
lp_url_list : list = []
kwlps : dict = {}
json_dict : dict = {"kusdt":None}
lp_json_dict : dict = {"kusdt":None}
#5분
prices_dict : dict = {"klay":[]}
prices_candle_dict : dict = {"Time": "", "klay":[]}
#15분
prices_dict_fifteen : dict = {"klay":[]}
prices_candle_dict_fifteen : dict = {"Time": "", "klay":[]}
#1시간
prices_dict_hour : dict = {"klay":[]}
prices_candle_dict_hour : dict = {"Time": "", "klay":[]}
#4시간
prices_dict_four_hour : dict = {"klay":[]}
prices_candle_dict_four_hour : dict = {"Time": "", "klay":[]}
#1일
prices_dict_day : dict = {"klay":[]}
prices_candle_dict_day : dict = {"Time": "", "klay":[]}

price_db = None

for i, name in enumerate(token_name_list):
    kwlps[name] = token_hash_list[i]

for k in kwlps.keys():
    json_dict[k] = None
    lp_json_dict[k] = None
    prices_dict[k] = []
    prices_candle_dict[k] = []
    prices_dict_fifteen[k] = []
    prices_candle_dict_fifteen[k] = []
    prices_dict_hour[k] = []
    prices_candle_dict_hour[k] = []
    prices_dict_four_hour[k] = []
    prices_candle_dict_four_hour[k] = []
    prices_dict_day[k] = []
    prices_candle_dict_day[k] = []

for k in prices_dict.keys():
    if k == "klay":
        url_list.append(f'https://api-cypress.scope.klaytn.com/v1/accounts/{klay_usdt_lp}/balances')
        lp_url_list.append(f'https://api-cypress.scope.klaytn.com/v1/accounts/{klay_usdt_lp}')
    else:
        url_list.append(f'https://api-cypress.scope.klaytn.com/v1/accounts/{kwlps[k]}/balances')
        lp_url_list.append(f'https://api-cypress.scope.klaytn.com/v1/accounts/{kwlps[k]}')

async def load_coin_json(url):
    try:
        async with ClientSession() as session:
            async with session.get(url) as response:
                raw_data = await response.read()
                r = json.loads(raw_data)
                if klay_usdt_lp == url[url.find("accounts/")+len("accounts/"):url.find("/balances")]:
                    json_dict[list(r['tokens'].values())[0]["symbol"].lower()] = r

                for k, v in kwlps.items():
                    if url[url.find("accounts/")+len("accounts/"):url.find("/balances")] == v:
                        # json_dict[list(r['tokens'].values())[0]["symbol"].lower()] = r
                        json_dict[k] = r
                        break
        return True
    except Exception as e:
        print(f"{datetime.datetime.now().strftime('%m/%d %H:%M')} : {e}")
        return False
              
async def load_lp_json(url):
    try:
        async with ClientSession() as session:
            async with session.get(url) as response:
                raw_data = await response.read()
                r = json.loads(raw_data)
                if r["result"]["address"] != klay_usdt_lp.lower():
                    if r["result"]["tokenName"][r["result"]["tokenName"].find("-")+1:].lower() == "kusdt":
                        lp_json_dict[r["result"]["tokenName"][r["result"]["tokenName"].find("KlaySwap LP ")+12:r["result"]["tokenName"].find("-")].lower()] = r
                    else:    
                        lp_json_dict[r["result"]["tokenName"][r["result"]["tokenName"].find("-")+1:].lower()] = r
                else:
                    lp_json_dict[r["result"]["tokenName"][r["result"]["tokenName"].find("-")+1:].lower()] = r
        return True
    except Exception as e:
        print(f"{datetime.datetime.now().strftime('%m/%d %H:%M')} : {e}")
        return False

def get_ratio(klay_info, tokn_info):
    klay_decimals = 18
    klay_balance = float(klay_info['result']['balance'])/(10**klay_decimals)
    
    if len(list(tokn_info['tokens'].values())) > 1 and klay_balance == 0:
        tokn1_decimals = list(tokn_info['tokens'].values())[0]['decimals']
        tokn1_amount = tokn_info['result'][0]['amount']
        tokn1_balance = float(tokn1_amount)/(10**tokn1_decimals)
        
        tokn2_decimals = list(tokn_info['tokens'].values())[1]['decimals']
        tokn2_amount = tokn_info['result'][1]['amount']
        tokn2_balance = float(tokn2_amount)/(10**tokn2_decimals)

        if str(list(tokn_info['tokens'].values())[0]['symbol'].lower()) in ks_check_list:
            return 0, str(list(tokn_info['tokens'].values())[1]['symbol'].lower()), tokn2_balance / tokn1_balance
        elif str(list(tokn_info['tokens'].values())[0]['symbol'].lower()) == "kusdt":
            return 2, str(list(tokn_info['tokens'].values())[1]['symbol'].lower()), tokn1_balance / tokn2_balance
        elif str(list(tokn_info['tokens'].values())[1]['symbol'].lower()) == "kusdt":
            return 2, str(list(tokn_info['tokens'].values())[0]['symbol'].lower()), tokn2_balance / tokn1_balance
        else:
            return 0, str(list(tokn_info['tokens'].values())[0]['symbol'].lower()), tokn1_balance / tokn2_balance
    else:
        tokn_decimals = list(tokn_info['tokens'].values())[0]['decimals']
        tokn_amount = tokn_info['result'][0]['amount']
        tokn_balance = float(tokn_amount)/(10**tokn_decimals)
        return 1, "klay", klay_balance / tokn_balance

def save_prices_history(klay_info, tokn_info):
    prices = dict()
    checker : int = 1
    base_symbol : str = ""
    prices['Time'] = (datetime.datetime.now() + datetime.timedelta(hours = int(9))).strftime('%m/%d %H:%M')
    checker, base_symbol, ratio = get_ratio(klay_info["kusdt"], tokn_info["kusdt"])
    if ratio == 0:
        return {}
    prices[base_symbol] = round(1/ratio, 8)
    
    tmp_dict : dict = {}
    for token_name in token_name_list:
        tmp_dict[token_name] = tokn_info[token_name]

    for key, value in tmp_dict.items():
        if key != "kusdt":
            checker, base_symbol, lp_ratio = get_ratio(klay_info[key], value)
            if lp_ratio == 0:
                return {}
            if checker == 1:
                prices[key] = round(lp_ratio * prices[base_symbol], 8)
            elif checker == 0:
                prices[key] = round(lp_ratio * prices[base_symbol], 8)
            else:
                prices[key] = round(lp_ratio, 8)
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

async def main():
    global json_dict
    global lp_json_dict
    global prices_dict
    global prices_candle_dict
    global prices_dict_fifteen
    global prices_candle_dict_fifteen
    global prices_dict_hour
    global prices_candle_dict_hour
    global prices_dict_four_hour
    global prices_candle_dict_four_hour
    global prices_dict_day
    global prices_candle_dict_day
    global price_db

    index : int = 0
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
        db_index = price_db.coin.price.find({}).distinct('_id')
        if len(db_index) != 0:
            index = max(db_index) + 1
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
            prices_dict, prices_candle_dict = db_update_prices(price_db.coin.price, index, prices, prices_dict, prices_candle_dict)

            index += 1
            cnt =0
            if index != 0 and index % 3 == 0:
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

        tasks = []
        
        for url in url_list:
            task = asyncio.ensure_future(load_coin_json(url))
            tasks.append(task)
        
        for url in lp_url_list:
            task = asyncio.ensure_future(load_lp_json(url))
            tasks.append(task)
                
        result = await asyncio.gather(*tasks)

        if not all(result):
            cnt += 1

            json_dict = {}
            lp_json_dict = {}
            loop_end = (datetime.datetime.now() - start).total_seconds()

            delay_time = loop_time - loop_end
            if delay_time < 0:
                delay_time = 1

            await asyncio.sleep(delay_time)
            continue

        prices = save_prices_history(lp_json_dict, json_dict)
        if prices == {}:
            cnt += 1

            json_dict = {}
            lp_json_dict = {}
            loop_end = (datetime.datetime.now() - start).total_seconds()

            delay_time = loop_time - loop_end
            if delay_time < 0:
                delay_time = 1

            await asyncio.sleep(delay_time)
            continue

        for k in prices_dict.keys():
            prices_dict[k].append(prices[k])

        if not prices_candle_dict["Time"]:
            prices_candle_dict["Time"] = prices["Time"]
            prices_candle_dict_fifteen["Time"] = prices["Time"]
            prices_candle_dict_hour["Time"] = prices["Time"]
            prices_candle_dict_four_hour["Time"] = prices["Time"]
            prices_candle_dict_day["Time"] = prices["Time"]

            for k, v in prices_dict.items():
                prices_candle_dict[k].append([v[0], max(v), min(v), v[len(v)-1]])
                prices_dict_fifteen[k] = prices_candle_dict[k][0]
                prices_dict_hour[k] = prices_candle_dict[k][0]
                prices_dict_four_hour[k] = prices_candle_dict[k][0]
                prices_dict_day[k] = prices_candle_dict[k][0]
        else:
            prices_candle_dict["Time"] = prices["Time"]
            for k, v in prices_dict.items():
                prices_candle_dict[k][len(prices_candle_dict[k])-1] = [v[0], max(v), min(v), v[len(v)-1]]

        if not prices_dict_fifteen["klay"]:
            prices_candle_dict_fifteen["Time"] = prices["Time"]
            prices_candle_dict_hour["Time"] = prices["Time"]
            prices_candle_dict_four_hour["Time"] = prices["Time"]
            prices_candle_dict_day["Time"] = prices["Time"]
            for k in prices_dict.keys():    
                prices_dict_fifteen[k] = prices_candle_dict[k][0]
                prices_dict_hour[k] = prices_candle_dict[k][0]
                prices_dict_four_hour[k] = prices_candle_dict[k][0]
                prices_dict_day[k] = prices_candle_dict[k][0]

        for k in prices_dict.keys():
            tmp_fifteen = prices_dict_fifteen[k] + prices_candle_dict[k][0]
            result_fifteen = [tmp_fifteen[0], max(tmp_fifteen), min(tmp_fifteen), tmp_fifteen[len(tmp_fifteen)-1]]
            prices_candle_dict_fifteen[k] = [result_fifteen]
            prices_dict_fifteen[k] = result_fifteen

            tmp_hour = prices_dict_hour[k] + prices_candle_dict[k][0]
            result_hour = [tmp_hour[0], max(tmp_hour), min(tmp_hour), tmp_hour[len(tmp_hour)-1]]
            prices_candle_dict_hour[k] = [result_hour]
            prices_dict_hour[k] = result_hour

            tmp_four_hour = prices_dict_four_hour[k] + prices_candle_dict[k][0]
            result_four_hour = [tmp_four_hour[0], max(tmp_four_hour), min(tmp_four_hour), tmp_four_hour[len(tmp_four_hour)-1]]
            prices_candle_dict_four_hour[k] = [result_four_hour]
            prices_dict_four_hour[k] = result_four_hour

            tmp_day = prices_dict_day[k] + prices_candle_dict[k][0]
            result_day = [tmp_day[0], max(tmp_day), min(tmp_day), tmp_day[len(tmp_day)-1]]
            prices_candle_dict_day[k] = [result_day]
            prices_dict_day[k] = result_day

        price_db.coin.price.update_one({"_id":index}, {"$set" : prices_candle_dict}, upsert=True)
        price_db.coin.price_fifteen.update_one({"_id":fifteen_index}, {"$set" : prices_candle_dict_fifteen}, upsert=True)
        price_db.coin.price_hour.update_one({"_id":hour_index}, {"$set" : prices_candle_dict_hour}, upsert=True)
        price_db.coin.price_four_hour.update_one({"_id":four_hour_index}, {"$set" : prices_candle_dict_four_hour}, upsert=True)
        price_db.coin.price_day.update_one({"_id":day_index}, {"$set" : prices_candle_dict_day}, upsert=True)
        
        cnt += 1

        json_dict = {}
        lp_json_dict = {}

        loop_end = (datetime.datetime.now() - start).total_seconds()

        delay_time = loop_time - loop_end
        if delay_time < 0:
            delay_time = 1

        await asyncio.sleep(delay_time)

if __name__ == "__main__":
    py_ver = int(f"{sys.version_info.major}{sys.version_info.minor}")
    if py_ver > 37 and sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
