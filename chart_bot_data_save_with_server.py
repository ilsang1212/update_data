# -*- coding: utf-8 -*- 

import os
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
cal_loop : int = int(os.environ["CAL_LOOP"])

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
prices_dict : dict = {"klay":[]}
prices_candle_dict : dict = {"Time": "", "klay":[]}

price_db = None

for i, name in enumerate(token_name_list):
    kwlps[name] = token_hash_list[i]

for k in kwlps.keys():
    json_dict[k] = None
    lp_json_dict[k] = None
    prices_dict[k] = []
    prices_candle_dict[k] = []

for k in prices_dict.keys():
    if k == "klay":
        url_list.append(f'https://api-cypress.scope.klaytn.com/v1/accounts/{klay_usdt_lp}/balances')
        lp_url_list.append(f'https://api-cypress.scope.klaytn.com/v1/accounts/{klay_usdt_lp}')
    else:
        url_list.append(f'https://api-cypress.scope.klaytn.com/v1/accounts/{kwlps[k]}/balances')
        lp_url_list.append(f'https://api-cypress.scope.klaytn.com/v1/accounts/{kwlps[k]}')

async def load_coin_json(url):
    async with ClientSession() as session:
        async with session.get(url) as response:
            raw_data = await response.read()
            r = json.loads(raw_data)
            json_dict[list(r['tokens'].values())[0]["symbol"].lower()] = r

async def load_lp_json(url):
    async with ClientSession() as session:
        async with session.get(url) as response:
            raw_data = await response.read()
            r = json.loads(raw_data)
            lp_json_dict[r["result"]["tokenName"][r["result"]["tokenName"].find("-")+1:].lower()] = r

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
        return tokn1_balance / tokn2_balance
    else:
        tokn_decimals = list(tokn_info['tokens'].values())[0]['decimals']
        tokn_amount = tokn_info['result'][0]['amount']
        tokn_balance = float(tokn_amount)/(10**tokn_decimals)
        return klay_balance / tokn_balance

def save_prices_history(klay_info, tokn_info):
    prices = dict()
    prices['Time'] = (datetime.datetime.now() + datetime.timedelta(hours = int(9))).strftime('%m/%d %H:%M')
    ratio = get_ratio(klay_info["kusdt"], tokn_info["kusdt"])
    if ratio == 0:
        return {}
    prices['klay'] = round(1/ratio, 8)

    for key, value in tokn_info.items():
        if key != "kusdt":
            lp_ratio = get_ratio(klay_info[key], value)
            if lp_ratio == 0:
                return {}
            prices[key] = round(lp_ratio * prices['klay'], 8)
    
    return prices

def main():
    global json_dict
    global lp_json_dict
    global prices_dict
    global prices_candle_dict
    global price_db

    index : int = 0
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
        db_index = price_db.coin.price.find().sort([("_id",-1)]).limit(1)
        index = list(db_index)[0]["_id"]
    except:
        pass

    while True:
        try:
            if cnt == cal_loop:
                prices_candle_dict["Time"] = prices["Time"]
                for k, v in prices_dict.items():
                    prices_candle_dict[k].pop(0)
                    prices_candle_dict[k].append([v[0], max(v), min(v), v[len(v)-1]])
                
                for k, v in prices_dict.items():
                    prices_dict[k] = [v[len(v)-1]]

                if price_db.coin.price.count_documents({}) >= max_length * 1.05:
                    delete_db_index = price_db.coin.price.find().sort([("_id",1)]).limit(1)
                    first_index = list(delete_db_index)[0]["_id"]
                    price_db.coin.price.find_one_and_delete({"_id":first_index})

                price_db.coin.price.update_one({"_id":index}, {"$set" : prices_candle_dict}, upsert=True)

                index += 1
                cnt =0

            loop = asyncio.get_event_loop()
            tasks = []
            
            for url in url_list:
                task = asyncio.ensure_future(load_coin_json(url))
                tasks.append(task)
            
            for url in lp_url_list:
                task = asyncio.ensure_future(load_lp_json(url))
                tasks.append(task)

            loop.run_until_complete(asyncio.wait(tasks))

            prices = save_prices_history(lp_json_dict, json_dict)
            if prices == {}:
                time.sleep(loop_time)
                continue

            for k in prices_dict.keys():
                prices_dict[k].append(prices[k])
            
            if not prices_candle_dict["Time"]:
                prices_candle_dict["Time"] = prices["Time"]
                for k, v in prices_dict.items():
                    prices_candle_dict[k].append([v[0], max(v), min(v), v[len(v)-1]])
            else:
                prices_candle_dict["Time"] = prices["Time"]
                for k, v in prices_dict.items():
                    prices_candle_dict[k][len(prices_candle_dict[k])-1] = [v[0], max(v), min(v), v[len(v)-1]]

            price_db.coin.price.update_one({"_id":index}, {"$set" : prices_candle_dict}, upsert=True)

            cnt += 1

            json_dict = {}
            lp_json_dict = {}
            
        except:
            pass
        time.sleep(loop_time)

if __name__ == "__main__":
    main()
    
