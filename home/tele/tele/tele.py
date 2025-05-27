import json
import time
import logging
import asyncio
from telegram import Bot
from telegram.ext import Application, CommandHandler
import os
import sys
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')



def get_external_path(filename):
    # 取得執行檔所在的目錄（對於 onefile 模式，sys.argv[0] 是完整的 exe 路徑）
    exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    return os.path.join(exe_dir, filename)

# 取得資源檔案的路徑
config_file = get_external_path("config.json")
positions_file = get_external_path("positions.json")
subscribers_file = get_external_path("subscribers.json")
check_freq = 5
#------------------------------------\
#讀取config.json檔案
with open(config_file, 'r',encoding='utf-8') as f:
    config = json.load(f)

#讀取config.json檔案的變數
mode = config.get("mode", 0)  
isSuccessClose = config.get("isSuccessClose", 0)  

TOKEN = config["TOKEN"]
bot = Bot(TOKEN)


async def subscribe(update, context):
    try:
        chat_id = update.message.chat_id
        subscribers = load_subscribers()
        
        if chat_id not in subscribers:
            subscribers.append(chat_id)
            save_subscribers(subscribers)
            await update.message.reply_text("You have successfully subscribed!")
            logging.info(f"New subscriber added: {chat_id}")
        else:
            await update.message.reply_text("You are already subscribed!")
            logging.info(f"Subscriber {chat_id} is already in the list.")
    except Exception as e:
        logging.error(f"An error occurred while subscribing: {e}")


def load_subscribers():
    try:
        with open(subscribers_file, 'r') as file:
            if file.read().strip() == '': 
                logging.warning(f"{subscribers_file} is empty, initializing with an empty list.")
                return []
            file.seek(0)  
            subscribers = json.load(file)
            logging.info("Subscribers loaded successfully.")
            return subscribers
    except FileNotFoundError:
        logging.warning(f"{subscribers_file} not found, creating a new one.")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing {subscribers_file}: {e}")
        return []


# 保存订阅者列表
def save_subscribers(subscribers):
    with open(subscribers_file, 'w') as file:
        json.dump(subscribers, file)
    logging.info(f"Subscribers saved to {subscribers_file}.")
    
# /unsubscribe 命令
async def unsubscribe(update, context):
    chat_id = update.message.chat_id
    subscribers = load_subscribers()
    
    if chat_id in subscribers:
        subscribers.remove(chat_id)
        save_subscribers(subscribers)
        await update.message.reply_text("You have successfully unsubscribed!")
        logging.info(f"Subscriber removed: {chat_id}")
    else:
        await update.message.reply_text("You are not subscribed!")
        logging.info(f"Subscriber {chat_id} is not in the list.")
        

def check_logs_for_errors(log_path_names):
    error_messages = []
    positions = {}
    has_error = False
    
    try:
        with open(positions_file, 'r') as file:
            if file.read().strip() == '': 
                logging.warning(f"positions.json is empty, initializing with an empty dictionary.")
                positions = {}
            else:
                file.seek(0)
                positions = json.load(file)
    except FileNotFoundError:
        logging.warning(f"positions.json not found, creating a new one.")
        positions = {}
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing positions.json: {e}")
        positions = {}

    for log_path_names in log_path_names:
        position = positions.get(log_path_names, 0)
        log_path = get_external_path(log_path_names)
        with open(log_path, 'r') as file:
            file.seek(position)
            for line in file:
                if 'ERROR' in line:
                    has_error = True
                    if mode == 0: 
                        error_messages.append(f"Error in {log_path_names}: {line.strip()}")
            positions[log_path_names] = file.tell()
            
    with open(positions_file, 'w') as file:
        json.dump(positions, file)

    if mode == 1 and has_error:  
        error_messages.append(f"Errors detected in one or more log files.")

    return error_messages, has_error


async def broadcast_errors(errors):
    subscribers = load_subscribers()
    if not subscribers:
        logging.info("No subscribers to broadcast to.")
        return
    for subscriber in subscribers:
        for error in errors:
            try:
                await bot.send_message(chat_id=subscriber, text=error)
                logging.info(f"Sent to {subscriber}: {error}")
            except Exception as e:
                logging.error(f"Failed to send message to {subscriber}: {e}")


async def broadcast_success():
    subscribers = load_subscribers()
    for subscriber in subscribers:
        try:
            await bot.send_message(chat_id=subscriber, text="No errors found, system running smoothly.")
            logging.info(f"Sent to {subscriber}: No errors found.")
        except Exception as e:
            logging.error(f"Failed to send success message to {subscriber}: {e}")

async def main():
    log_paths = config["LOG_PATH"]
    print("log_paths:", log_paths)
    while True:
        errors, has_error = check_logs_for_errors(log_paths)
        if errors:
            await broadcast_errors(errors)
        elif isSuccessClose == 1 and not has_error:
            logging.info("No errors found, sending success message.")
            await broadcast_success()
            break  
        await asyncio.sleep(check_freq) 

if __name__ == '__main__':
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("subscribe", subscribe))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe))

    logging.info("Bot is running. Send /subscribe to test.")

    loop = asyncio.get_event_loop()

    loop.create_task(main())

    application.run_polling()

    print("......")
