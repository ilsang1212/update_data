# -*- coding: utf-8 -*- 

import os
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from telegram import Update
import numpy as np
import datetime, random, time
from pymongo import MongoClient
import pymongo, ssl
import requests
from googletrans import Translator

token = os.environ["BOT_TOKEN"]
chat_id_list : list = os.environ["CHAT_ID_LIST"].split(" ")
owner : list = os.environ["OWNER"].split(" ")
client_id = os.environ["CLIENT_ID"]
client_secret = os.environ["CLIENT_SECRET"]

# mongoDB_connect_info : dict = {
#     "host" : os.environ["mongoDB_HOST"],
#     "username" : os.environ["USER_ID"],
#     "password" : os.environ["USER_PASSWORD"]
#     }

translator = Translator()

def test(update, ctx):
    ctx.bot.send_message(chat_id=update.message.chat_id, text=f"{update.message.chat_id}")

def get_message(update, ctx):
    pass

def katto(update, ctx):
    if str(update.message.chat_id) in ["-1001639722491"]:
        ctx.bot.send_message(chat_id=update.message.chat_id, text="여기서 쓰지마라고.!")
        return
    result_str : str = ""
    sample_list : list = []
    emoji_list : list = ["0️⃣", "1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣"]
    for i in range(25):
        sample_list.append(random.randint(0, 9))

    for data in (random.sample(sample_list, 5)):
        result_str += f"{emoji_list[data]} "
        
    ctx.bot.send_message(chat_id=update.message.chat_id, text=result_str)

def race(update, ctx):
#     print(str(update.message.from_user["username"]))
    if str(update.message.chat_id) not in chat_id_list or str(update.message.from_user["username"]) not in owner:
        ctx.bot.send_message(chat_id=update.message.chat_id, text="사용할 수 없습니다. 밬호드님께 요청하세요")
        return
    msg = update.message.text
    race_info = []
    fr = []
    racing_field = []
    str_racing_field = []
    cur_pos = []
    race_val = []
    random_pos = []
    racing_result = []
    output = '📷 📷 📷 신나는 레이싱! 📷 📷 📷\n'
    racing_unit = ['🚗','🚕','🚙','🛻','🚌','🚎','🏎️','🚓','🚑','🚒','🚐','🦽','🛺','🛵','🛸','🚲', '🐦', '🐌', '🐢', '🐈', '🦖', '🐳', '🐅', '🦣', '🐆', '🐖', '🐁', '🦃']
    random.shuffle(racing_unit)
    racing_member = msg.split(" ")
    racing_member.pop(0)

    field_size = 52
    tmp_race_tab = 20 - len(racing_member)
    if len(racing_member) <= 1:
        return
    elif len(racing_member) >= 15:
        return
    else :
        race_val = random.sample(range(tmp_race_tab, tmp_race_tab+len(racing_member)), len(racing_member))
        random.shuffle(race_val)
        for i in range(len(racing_member)):
            fr.append(racing_member[i])
            fr.append(racing_unit[i])
            fr.append(race_val[i])
            race_info.append(fr)
            fr = []
            for i in range(field_size):
                fr.append(" ")
            racing_field.append(fr)
            fr = []

        for i in range(len(racing_member)):
            racing_field[i][0] = "|"
            racing_field[i][field_size-2] = race_info[i][1]
            if len(race_info[i][0]) > 5:
                racing_field[i][field_size-1] = "| " + race_info[i][0][:5] + '..'
            else:
                racing_field[i][field_size-1] = "| " + race_info[i][0]
            str_racing_field.append("".join(racing_field[i]))
            cur_pos.append(field_size-2)
        
        for i in range(len(racing_member)):
            output +=  str_racing_field[i] + '\n'

        result_race = update.message.reply_text(f"{output} 🚥 3초 후 경주가 시작됩니다!")
        time.sleep(1)
        result_race.edit_text(f"{output} 🚥 2초 후 경주가 시작됩니다!")
        time.sleep(1)
        result_race.edit_text(f"{output} 🚥 1초 후 경주가 시작됩니다!")
        time.sleep(1)
        result_race.edit_text(f"{output} 🏁  경주 시작!")

        for i in range(len(racing_member)):
            test = random.sample(range(2,field_size-2), race_info[i][2])
            while len(test) != tmp_race_tab + len(racing_member)-1 :
                test.append(1)
            test.append(1)
            test.sort(reverse=True)
            random_pos.append(test)

        for j in range(len(random_pos[0])):
            if j%2 == 0:
                output =  '📷 📸 📷 신나는 레이싱! 📸 📷 📸\n'
            else :
                output =  '📸 📷 📸 신나는 레이싱! 📷 📸 📷\n'
            str_racing_field = []
            for i in range(len(racing_member)):
                temp_pos = cur_pos[i]
                racing_field[i][random_pos[i][j]], racing_field[i][temp_pos] = racing_field[i][temp_pos], racing_field[i][random_pos[i][j]]
                cur_pos[i] = random_pos[i][j]
                str_racing_field.append("".join(racing_field[i]))

            time.sleep(0.8)

            for i in range(len(racing_member)):
                output +=  str_racing_field[i] + '\n'
            
            result_race.edit_text(f"{output} 🏁  경주 시작!")		
        
        for i in range(len(racing_field)):
            fr.append(race_info[i][0])
            fr.append((race_info[i][2]) - tmp_race_tab + 1)
            racing_result.append(fr)
            fr = []

        result = sorted(racing_result, key=lambda x: x[1])

        result_str = ''
        for i in range(len(result)):
            if result[i][1] == 1:
                result[i][1] = '🥇'
            elif result[i][1] == 2:
                result[i][1] = '🥈'
            elif result[i][1] == 3:
                result[i][1] = '🥉'
            elif result[i][1] == 4:
                result[i][1] = '4️⃣'
            elif result[i][1] == 5:
                result[i][1] = '5️⃣'
            elif result[i][1] == 6:
                result[i][1] = '6️⃣'
            elif result[i][1] == 7:
                result[i][1] = '7️⃣'
            elif result[i][1] == 8:
                result[i][1] = '8️⃣'
            elif result[i][1] == 9:
                result[i][1] = '9️⃣'
            elif result[i][1] == 10:
                result[i][1] = '🔟'
            else:
                result[i][1] = '❌'
            result_str += result[i][1] + "  " + result[i][0] + "  "
            
        #print(result)
        time.sleep(1)
        result_race.edit_text(f"{output} 🎉 경주 종료!\n{result_str}")
        return
        # return await result_race.edit(content = output + '🎉 경주 종료!\n' + result_str)

def translate_txt_papago(update, ctx):
    if str(update.message.chat_id) not in chat_id_list:
        return

    # print(ctx.args)
    # 네이버 Papago 언어감지 API 예제
    input_text = update.message.text[7:]

    if len(input_text) > 1500:
        ctx.bot.send_message(chat_id=update.message.chat_id, text="1500자가 넘습니다.")
        return

    header = {"X-Naver-Client-Id":client_id,
            "X-Naver-Client-Secret":client_secret}

    data = {'query' : input_text}

    url = "https://openapi.naver.com/v1/papago/detectLangs"
    response_langCode = requests.post(url, headers=header, data=data)
    rescode = response_langCode.status_code
    if(rescode==200):
        response_langCode_result = response_langCode.json()
        print(response_langCode_result)
        # print(response_langCode.decode('utf-8'))
    else:
        print("Error Code:" + rescode)

    data = {'text' : input_text,
            'source' : response_langCode_result['langCode'],
            'target': 'ko'}
    url = "https://openapi.naver.com/v1/papago/n2mt"

    response_translate = requests.post(url, headers=header, data=data)
    rescode = response_translate.status_code
    if(rescode==200):
        response_translate_result = response_translate.json()
        result = update.message.reply_text(f"{response_translate_result['message']['result']['translatedText']}")
    else:
        print("Error Code:" + rescode)

def translate_txt(update, ctx):
    if str(update.message.chat_id) not in chat_id_list:
        return

    input_text = update.message.text[7:]

    translation = translator.translate(input_text, dest="ko")

    update.message.reply_text(f"{translation.text}")
    return

def main():
#     try:
#         print(mongoDB_connect_info)
#         price_db = MongoClient(ssl=True, tlsAllowInvalidCertificates=True, **mongoDB_connect_info)
#         price_db.admin.command("ismaster") # 연결 완료되었는지 체크
#         print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\ndb 연결 완료. 아이디:{mongoDB_connect_info['username']}")
#     except pymongo.errors.ServerSelectionTimeoutError:
#         print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\ndb 연결 실패! host 리스트를 확인할 것.")
#     except pymongo.errors.OperationFailure:
#         print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\ndb 로그인 실패! username과 password를 확인할 것.")
#     except Exception as e:
#         print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\ndb 연결 실패! 오류 발생: {e}")

    updater = Updater(token, use_context=True)
    dp = updater.dispatcher
    print("Bot Started")

    # message reply function
    # message_handler = MessageHandler(Filters.text & (~Filters.command), get_message)

    # dp.add_handler(message_handler)
    dp.add_handler(CommandHandler(["test"], test))
    dp.add_handler(CommandHandler(["race"], race))
    dp.add_handler(CommandHandler(["katto", "ka", "kt", "lo", "lotto"], katto))
    dp.add_handler(CommandHandler(["trans"], translate_txt_papago))
    # dp.add_handler(MessageHandler(Filters.command, unknown))

    updater.start_polling(timeout=3, clean=True)
    updater.idle()

if __name__ == "__main__":
    main()
    
