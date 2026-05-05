import os
import math
import asyncio
import logging
import random
import html
import uuid
import gc
import json
import concurrent.futures
from datetime import datetime, date, timedelta, timezone
from aiogram import Bot, Dispatcher, types, F

IST = timezone(timedelta(hours=5, minutes=30))
import aiohttp
import urllib.parse
from aiogram.filters import Command, CommandObject
from aiogram.types import ChatMemberUpdated, ErrorEvent
from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter, IS_NOT_MEMBER, MEMBER, ADMINISTRATOR
from supabase import create_client, Client
from aiogram.exceptions import TelegramRetryAfter
from aiogram.types import BufferedInputFile

from dotenv import load_dotenv

# This loads a local .env file if you are testing on your computer
load_dotenv() 

# --- 1. CREDENTIALS (SECURED) ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
MINI_APP_URL = "https://midnight-casino-bot.netlify.app"
MAIN_GROUP_ID = -1003813960922
LOG_GROUP_ID = -1003519381734
SYS_LOG_GROUP_ID = -1003519381734
MY_BOT_USERNAME = "MidnightCasinoBot" # 👈 Change this if your bot's username is different!
AD_TOKENS = {}
EARN_COOLDOWNS = {} # 👈 ADD THIS: Tracks the 5-minute /earn cooldown

# --- ADSGRAM CREDENTIALS ---
ADSGRAM_BLOCK_ID = os.getenv("ADSGRAM_BLOCK_ID") or "23671"
ADSGRAM_TOKEN = os.getenv("ADSGRAM_TOKEN") or "b1231df256914e13936b24ad03576743"

def generate_ad_link(user_id: int, action: str):
    """Creates a secure, 1-time ticket for a specific reward action."""
    ticket = str(uuid.uuid4())[:8]
    AD_TOKENS[ticket] = {
        "user_id": user_id, 
        "action": action,
        "ts": datetime.now().timestamp()
    }
    return f"{MINI_APP_URL}?bot={MY_BOT_USERNAME}&token={ticket}"

# Safety check: The bot will crash instantly with a helpful error if Stackhost is missing the keys
if not all([BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    raise ValueError("🚨 MISSING CREDENTIALS! Check your Stackhost Environment Variables.")

async def send_log(text: str):
    """Fires a silent log to the admin group without slowing down the bot."""
    if not LOG_GROUP_ID: return
    try:
        await bot.send_message(LOG_GROUP_ID, text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        logging.warning(f"Log failed: {e}")

async def send_sys_log(text: str, document=None):
    """Fires a system/error log or backup file to the Dev/System group."""
    if not SYS_LOG_GROUP_ID: return
    try:
        if document:
            await bot.send_document(SYS_LOG_GROUP_ID, document=document, caption=text, parse_mode="HTML")
        else:
            await bot.send_message(SYS_LOG_GROUP_ID, text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        logging.warning(f"Sys Log failed: {e}")


# --- 2. INITIALIZATION ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 3. HIGH SPEED RAM CACHE ---
PLAYER_CACHE = {}
# --- ADD THIS NEAR YOUR OTHER CACHES (Top of file) ---
CHARACTER_CACHE = {} # Stores all waifus in RAM for 0ms latency

# Rarity configurations (Gold per hour)
RARITY_RATES = {
    "Common": 500,
    "Rare": 1000,
    "Epic": 5000,
    "Legendary": 10000,
    "Mythic": 50000
}

# --- GACHA 2.0 BANNER CONFIGURATION ---
SUMMON_TIERS = {
    "standard": {
        "name": "Standard", "cost": 500000, "emoji": "🥉",
        "desc": "The standard street draft.",
        "rates": {"Common": 70.0, "Rare": 22.0, "Epic": 7.0, "Legendary": 0.99, "Mythic": 0.01}
    },
    "elite": {
        "name": "Elite", "cost": 1000000, "emoji": "🥈",
        "desc": "🚫 NO Commons.",
        "rates": {"Rare": 65.0, "Epic": 30.0, "Legendary": 4.0, "Mythic": 1.0}
    },
    "executive": {
        "name": "Executive", "cost": 2000000, "emoji": "🥇",
        "desc": "🚫 NO Commons. 🚫 NO Rares.",
        "rates": {"Epic": 85.0, "Legendary": 10.0, "Mythic": 5.0}
    }
}


# --- 1. Define the New Rarity Ranges ---
RARITY_RANGES = {
    "Common": (10, 40),
    "Rare": (30, 60),
    "Epic": (50, 80),
    "Legendary": (70, 100),
    "Mythic": (90, 125)
}

# --- THE UNDERWORLD (5x5 PUB MATRIX) ---
PUB_MATRIX = {
    "coin": [
        {"name": "The Rusty Toss", "money": 500, "tax": 0.10, "min_plays": 3},
        {"name": "Two-Face Tavern", "money": 1500, "tax": 0.15, "min_plays": 4},
        {"name": "The Silver Quarter", "money": 3500, "tax": 0.20, "min_plays": 5},
        {"name": "The Golden Flip", "money": 7500, "tax": 0.30, "min_plays": 7},
        {"name": "Emperor's Edge", "money": 15000, "tax": 0.45, "min_plays": 10},
    ],
    "dice": [
        {"name": "Snake Eyes Alley", "money": 500, "tax": 0.10, "min_plays": 3},
        {"name": "The Loaded Bone", "money": 1500, "tax": 0.15, "min_plays": 4},
        {"name": "The Lucky 7 Club", "money": 3500, "tax": 0.20, "min_plays": 5},
        {"name": "High Roller's Crapshoot", "money": 7500, "tax": 0.30, "min_plays": 7},
        {"name": "The Devil's Cubes", "money": 15000, "tax": 0.45, "min_plays": 10},
    ],
    "slots": [
        {"name": "The Broken Bandit", "money": 500, "tax": 0.10, "min_plays": 3},
        {"name": "The Neon Spin", "money": 1500, "tax": 0.15, "min_plays": 4},
        {"name": "Jackpot Junction", "money": 3500, "tax": 0.20, "min_plays": 5},
        {"name": "The Diamond Reel", "money": 7500, "tax": 0.30, "min_plays": 7},
        {"name": "Syndicate 777", "money": 15000, "tax": 0.45, "min_plays": 10},
    ],
    "mines": [
        {"name": "The Powder Keg", "money": 500, "tax": 0.10, "min_plays": 3},
        {"name": "Sweeper's Den", "money": 1500, "tax": 0.15, "min_plays": 4},
        {"name": "Boom Town", "money": 3500, "tax": 0.20, "min_plays": 5},
        {"name": "The Hurt Locker", "money": 7500, "tax": 0.30, "min_plays": 7},
        {"name": "Oppenheimer's Lounge", "money": 15000, "tax": 0.45, "min_plays": 10},
    ],
    "cards": [
        {"name": "Joker's Wild", "money": 500, "tax": 0.10, "min_plays": 3},
        {"name": "The Dealer's Den", "money": 1500, "tax": 0.15, "min_plays": 4},
        {"name": "Black Heart Tavern", "money": 3500, "tax": 0.20, "min_plays": 5},
        {"name": "The Ace of Spades", "money": 7500, "tax": 0.30, "min_plays": 7},
        {"name": "King's Bluff Casino", "money": 15000, "tax": 0.45, "min_plays": 10},
    ]
}

# --- DYNAMIC CARD MULTIPLIERS ---
CARD_MULTIPLIERS = {
    "2":  {"high": 1.2,  "low": 0.0},
    "3":  {"high": 1.3,  "low": 12.0},
    "4":  {"high": 1.4,  "low": 6.0},
    "5":  {"high": 1.5,  "low": 4.0},
    "6":  {"high": 1.7,  "low": 3.0},
    "7":  {"high": 1.8,  "low": 2.2},
    "8":  {"high": 2.0,  "low": 2.0},
    "9":  {"high": 2.2,  "low": 1.8},
    "10": {"high": 3.0,  "low": 1.7},
    "J":  {"high": 4.0,  "low": 1.5},
    "Q":  {"high": 6.0,  "low": 1.4},
    "K":  {"high": 12.0, "low": 1.3},
    "A":  {"high": 0.0,  "low": 1.2}
}


BOUNTY_TARGET_ID = None
BOUNTY_AMOUNT = 0
LAST_BOUNTY_TARGET_ID = None
LAST_BOUNTY_AMOUNT = 0
LAST_BOUNTY_CLAIMER_ID = None

mine_games = {}      
card_games = {}      
coin_games = {}
dice_games = {}
rps_games = {} 
duel_games = {}   # <--- ADD THIS
draft_games = {}  # <--- ADD THIS
active_games = set() 
brawl_games = {}
crate_games = {}
challenge_games = {}
aeroplane_games = {}
roulette_games = {}
poker_games = {}
heist_games = {}
racing_games = {}

RACING_THEMES = {
    "cars": {
        "name": "Midnight Street Drag", "icon": "🏎️", 
        "racers": ["🔴 Red Demon", "🔵 Blue Comet", "🟢 Toxic Green", "🟡 Golden Boy", "🟣 Midnight Purple"]
    },
    "bikes": {
        "name": "Alleyway Scramble", "icon": "🏍️", 
        "racers": ["🟥 Street Glide", "🟦 Alley Rat", "🟩 Neon Ninja", "🟨 Sandstorm", "🟪 The Phantom"]
    },
    "horses": {
        "name": "The Syndicate Derby", "icon": "🐎", 
        "racers": ["🐎 Thunderbolt", "🐎 Widowmaker", "🐎 Silver Bullet", "🐎 Royal Flush", "🐎 Dark Horse"]
    }
}

active_drop_tasks = {}
verified_hackers = {}

# --- CRATES MULTIPLIERS (0-50% Scrap Refund Included) ---
CRATE_MULTIPLIERS = {
    3: 2.35,
    4: 3.05,
    5: 3.75,
    6: 4.45,
    7: 5.15,
    8: 5.85,
    9: 6.55
}

# --- BRAWL COMBAT STATS (2.5x Compressed Scale) ---
BRAWL_STATS = {
    "Common": {"hp": 100, "dmg": 20, "spd": 10},
    "Rare": {"hp": 120, "dmg": 25, "spd": 20},
    "Epic": {"hp": 150, "dmg": 32, "spd": 30},
    "Legendary": {"hp": 190, "dmg": 40, "spd": 40},
    "Mythic": {"hp": 250, "dmg": 50, "spd": 50}
}

CARDS = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]

GACHA_BOOSTS = {"Common": 0, "Rare": 5, "Epic": 10, "Legendary": 15, "Mythic": 25}
EMOJIS = {"Common": "🤍", "Rare": "💙", "Epic": "💜", "Legendary": "💛", "Mythic": "💖"}

# --- 4. DATABASE DEBOUNCER SYSTEM (ANTI-CRASH) ---
# Merges rapid updates into a single DB call to prevent Memory Overflows.
PENDING_DB_UPDATES = {}

async def db_worker():
    """Flushes aggregated DB updates concurrently in safe batches to keep RAM completely empty."""
    while True:
        await asyncio.sleep(3.0) 
        
        try:
            if not PENDING_DB_UPDATES:
                continue
                
            tables = list(PENDING_DB_UPDATES.keys())
            for table in tables:
                # 1. Pop the data out of the main RAM queue immediately
                records = PENDING_DB_UPDATES.pop(table, {})
                tasks = []
                
                for match_val, payload in records.items():
                    try:
                        match_col = payload.pop("_match_col")
                        # 2. Add to a batch list instead of waiting for it to finish
                        task = async_db(supabase.table(table).update(payload).eq(match_col, match_val))
                        tasks.append(task)
                        
                        # 3. Flush to Supabase in batches of 5 (Matches your Semaphore)
                        if len(tasks) >= 5:
                            await asyncio.gather(*tasks, return_exceptions=True)
                            tasks.clear()
                            
                    except Exception as inner_e:
                        logging.error(f"❌ DB Worker Update Error ({match_val}): {inner_e}")
                
                # 4. Flush any remaining updates
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                
                # 5. FORCE DELETE the processed records from RAM
                del records 
                        
        except Exception as e:
            logging.error(f"❌ DB Worker Fatal Error (Recovering): {e}")

def queue_db_update(table, data, match_col, match_val):
    """Stores the latest state in RAM. The worker will flush it to the DB."""
    if table not in PENDING_DB_UPDATES:
        PENDING_DB_UPDATES[table] = {}
        
    if match_val not in PENDING_DB_UPDATES[table]:
        PENDING_DB_UPDATES[table][match_val] = {"_match_col": match_col}
        
    for k, v in data.items():
        PENDING_DB_UPDATES[table][match_val][k] = v

async def daily_drop_task():
    """Drops a high-value code once a day between 9 AM and 9 PM IST."""
    while True:
        now = datetime.now(IST)
        
        # If we are past 9 PM, sleep until 9 AM tomorrow
        if now.hour >= 21:
            next_run = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
            await asyncio.sleep((next_run - now).total_seconds())
            continue
            
        # If we are before 9 AM, sleep until 9 AM
        if now.hour < 9:
            next_run = now.replace(hour=9, minute=0, second=0, microsecond=0)
            await asyncio.sleep((next_run - now).total_seconds())
            continue
            
        # We are between 9 AM and 9 PM. Pick a random drop time!
        end_of_window = now.replace(hour=21, minute=0, second=0, microsecond=0)
        seconds_left = int((end_of_window - now).total_seconds())
        
        if seconds_left > 0:
            wait_time = random.randint(0, seconds_left)
            await asyncio.sleep(wait_time)
            
            # --- EXECUTE DROP ---
            chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            code_suffix = "".join(random.choice(chars) for _ in range(6))
            promo_code = f"MIDNIGHT-{code_suffix}"
            reward_gold = random.randint(1_000_000, 10_000_000)
            
            await async_db(supabase.table("promo_codes").upsert({
                "code": promo_code.lower(),
                "reward_gold": reward_gold,
                "reward_commons": 0,
                "max_uses": 5,
                "uses": 0
            }))
            
            bot_me = await bot.get_me()
            kb = types.InlineKeyboardMarkup(inline_keyboard=[[
                types.InlineKeyboardButton(text="🔓 Hack the Firewall", url=f"https://t.me/{bot_me.username}?start=drop_{promo_code}")
            ]])
            
            try:
                await bot.send_message(
                    MAIN_GROUP_ID,
                    f"🚨 <b>𝗦𝗬𝗡𝗗𝗜𝗖𝗔𝗧𝗘 𝗗𝗥𝗢𝗣 𝗜𝗡𝗧𝗘𝗥𝗖𝗘𝗣𝗧𝗘𝗗</b> 🚨\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"💰 <b>Bounty:</b> {reward_gold:,} Gold\n"
                    f"📦 <b>Claims Available:</b> 5\n\n"
                    f"<i>First 5 players to hack the firewall get the code!</i>",
                    reply_markup=kb, parse_mode="HTML"
                )
            except Exception as e:
                logging.error(f"Drop error: {e}")
                
        # After dropping, sleep until 9 AM the next day to prevent multiple drops
        now_after = datetime.now(IST)
        tomorrow = (now_after + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        await asyncio.sleep((tomorrow - now_after).total_seconds())

@dp.message(F.chat.type == "private", F.text, ~F.text.startswith("/"))
async def handle_private_messages(m: types.Message):
    """Catches user responses for the Cipher hacking task."""
    if m.from_user.id in active_drop_tasks:
        task = active_drop_tasks[m.from_user.id]
        guess = m.text.strip().upper()
        
        if guess == task['word']:
            code = task['code']
            del active_drop_tasks[m.from_user.id]
            
            # --- THE FIX: ADD THEM TO THE WHITELIST ---
            if m.from_user.id not in verified_hackers:
                verified_hackers[m.from_user.id] = set()
            verified_hackers[m.from_user.id].add(code.lower())
            
            await m.answer(
                f"✅ <b>𝗙𝗜𝗥𝗘𝗪𝗔𝗟𝗟 𝗕𝗬𝗣𝗔𝗦𝗦𝗘𝗗!</b>\n\n"
                f"Your code is: <code>{code}</code>\n"
                f"<i>Hurry back to the main group and type /redeem {code} before the 5 claims are gone!</i>",
                parse_mode="HTML"
            )
        else:
            await m.answer("❌ <b>ACCESS DENIED.</b> Incorrect cipher. Try again.", parse_mode="HTML")

# --- STRICT DATABASE CONNECTION LIMITER ---
# This prevents the host from killing the bot due to sudden RAM/Thread spikes.
GLOBAL_DB_SEMAPHORE = asyncio.Semaphore(5)

async def async_db(query):
    """Executes a Supabase query in a thread, strictly limited to prevent OOM Kills."""
    async with GLOBAL_DB_SEMAPHORE:
        return await asyncio.to_thread(query.execute)

async def get_player_fast(user: types.User):
    uid = user.id
    
    # 1. REMOVED THE 15-SECOND REFRESH RACE CONDITION
    if uid in PLAYER_CACHE:
        return PLAYER_CACHE[uid]

    username = user.username if user.username else f"User_{user.id}"
    try:
        res = await async_db(supabase.table("players").select("*").eq("id", uid))
        if not res.data:
            db_data = {
                "id": uid,
                "username": username,
                "level": 1,
                "xp": 0,
                "retro_gold_paid": True,
                "balance": 1000,
                "wins": 0,          
                "total_losses": 0,  
                "robs_won": 0,
                "robs_lost": 0,
                "bank_ads_count": 0,
                "active_defense": "None",
                "defense_health": 0,
                "has_used_ad_spin": False,
                "ad_spin_unlocked": False,
                "in_pub": False,
                "pub_name": None,
                "pub_game": None,
                "pub_balance": 0,
                "pub_plays": 0,
                "pity_counter": 0,
                "last_explore_time": None
            }
            await async_db(supabase.table("players").insert(db_data))
            PLAYER_CACHE[uid] = db_data
            
            # --- NEW: SEND AUDIT LOG ---
            safe_name = html.escape(username)
            asyncio.create_task(send_log(f"🆕 <b>NEW CITIZEN</b>\n<a href='tg://user?id={uid}'>{safe_name}</a> just entered the Midnight Casino!"))
            
            return db_data

        # 1. DEFINE P_DATA FIRST
        p_data = res.data[0]

        # 2. THEN DO THE BAN CHECK
        if p_data.get('is_banned'):
            return None # The bot will silently ignore them

        # 3. THEN AUTO-FIX OLD PLAYERS
        if p_data.get("username", "").startswith("User_") and user.first_name:
            p_data["username"] = user.first_name
            queue_db_update("players", {"username": user.first_name}, "id", uid)
            
        # 1. FIX THEIR XP AND LEVEL FIRST
        expected_xp = p_data.get('wins', 0) * 10
        actual_xp = p_data.get('xp', 0)
        
        if actual_xp < expected_xp:
            p_data['xp'] = expected_xp
            p_data['level'] = (expected_xp // 100) + 1
            queue_db_update("players", {"xp": p_data['xp'], "level": p_data['level']}, "id", uid)

        # 2. PAY THEM THE GOLD THEY MISSED
        if not p_data.get('retro_gold_paid') or (actual_xp < expected_xp):
            final_level = p_data.get('level', 1)
            missed_gold = 0
            if final_level > 1:
                for lvl in range(2, final_level + 1):
                    missed_gold += 1000 * (lvl - 1)
            
            p_data['retro_gold_paid'] = True
            update_dict = {"retro_gold_paid": True}
            
            if missed_gold > 0:
                p_data['balance'] = p_data.get('balance', 0) + missed_gold
                update_dict["balance"] = p_data['balance']
                
                try:
                    asyncio.create_task(bot.send_message(
                        uid,
                        f"🎁 <b>VETERAN REWARD SECURED!</b>\n\n"
                        f"Your account was audited. You are officially <b>Level {final_level}</b>!\n"
                        f"💰 <b>Retroactive Payout:</b> {missed_gold:,} gold has been deposited into your vault.",
                        parse_mode="HTML"
                    ))
                except: pass
                    
            queue_db_update("players", update_dict, "id", uid)
        
        if p_data.get('has_used_ad_spin') is None: p_data['has_used_ad_spin'] = False
        if p_data.get('ad_spin_unlocked') is None: p_data['ad_spin_unlocked'] = False
        if p_data.get('in_pub') is None: p_data['in_pub'] = False
        if p_data.get('pub_balance') is None: p_data['pub_balance'] = 0
        if p_data.get('pub_plays') is None: p_data['pub_plays'] = 0
        if p_data.get('pity_counter') is None: p_data['pity_counter'] = 0
        if p_data.get('bank_balance') is None: p_data['bank_balance'] = 0

        PLAYER_CACHE[uid] = p_data
        return p_data
    except Exception as e:
        logging.error(f"❌ DB Error: {e}")
        return None

async def update_player_balance(user_id, amount_change, other_updates=None):
    # 1. Fetch the player into cache if they were cleared for inactivity
    if user_id not in PLAYER_CACHE:
        temp_user = types.User(id=user_id, is_bot=False, first_name="Unknown")
        p = await get_player_fast(temp_user)
        if not p: return # Complete database failure fallback
    else:
        p = PLAYER_CACHE[user_id]
    
    # 2. Update RAM
    p['balance'] += amount_change
    db_payload = {"balance": p['balance']}
    
    if other_updates:
        p.update(other_updates)
        db_payload.update(other_updates)
        
    # 3. Queue for DB Worker
    queue_db_update("players", db_payload, "id", user_id)

# --- 5. CLEANUP TASK (AGGRESSIVE RAM MANAGEMENT) ---
async def background_cleanup_task():
    while True:
        await asyncio.sleep(60) # Runs every 60 seconds
        try:
            now = datetime.now().timestamp()
            to_remove = []
          
            # 1. Clear dead mini-games
            for d in [mine_games, card_games, dice_games, coin_games, rps_games, crate_games, challenge_games, aeroplane_games, roulette_games, poker_games, heist_games, racing_games]:
                for uid, game in list(d.items()):
                    if isinstance(game, dict) and now - game.get('ts', now) > 600:
                        del d[uid]
                        to_remove.append(uid)
            
            # 2. Clear inactive players from the 'playing' status
            for uid in list(active_games):
                is_playing = (uid in mine_games or uid in card_games or uid in coin_games or 
                              uid in dice_games or uid in rps_games)
                if not is_playing:
                    to_remove.append(uid)
            
            for uid in set(to_remove):
                active_games.discard(uid)

            # 3. THE FIX: Aggressively clear the Player Cache
            # Changed from 5000 to 300 to keep RAM usage tiny
            if len(PLAYER_CACHE) > 300:
                keys = list(PLAYER_CACHE.keys())
                for k in keys:
                    if k not in active_games:
                        del PLAYER_CACHE[k]

            # 5. Clear expired Ad Tokens (RAM Leak Fix)
            expired_tokens = [k for k, v in AD_TOKENS.items() if now - v.get('ts', now) > 3600]
            for k in expired_tokens:
                del AD_TOKENS[k]

            # 6. Clear expired Earn Cooldowns (NEW)
            expired_cooldowns = [k for k, v in EARN_COOLDOWNS.items() if now - v > 300]
            for k in expired_cooldowns:
                del EARN_COOLDOWNS[k]

            # 4. THE NUCLEAR OPTION: Force Python to empty the trash
            gc.collect()

        except Exception as e:
            logging.error(f"Cleanup Error: {e}")

# --- 5.5 DATABASE AUTO-BACKUP TASK ---
async def auto_backup_task():
    """Runs every day at exactly Midnight IST to dump the DB to a JSON file."""
    while True:
        # Calculate time until Midnight IST
        now = datetime.now(IST)
        next_run = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        await asyncio.sleep((next_run - now).total_seconds())

        try:
            await send_sys_log("⏳ <b>Starting Daily Database Backup...</b>")
            
            backup_data = {}
            tables_to_backup = ["players", "characters", "promo_codes", "groups"]
            
            for table in tables_to_backup:
                start, limit = 0, 1000
                table_records = []
                while True:
                    res = await async_db(supabase.table(table).select("*").range(start, start + limit - 1))
                    if not res.data: break
                    table_records.extend(res.data)
                    if len(res.data) < limit: break
                    start += limit
                backup_data[table] = table_records
            
            # Convert to a formatted JSON file in RAM
            json_dump = json.dumps(backup_data, indent=4)
            file_bytes = json_dump.encode('utf-8')
            
            # Send the file directly to the System Log Telegram Group
            doc = BufferedInputFile(file_bytes, filename=f"Midnight_Backup_{now.strftime('%Y-%m-%d')}.json")
            await send_sys_log(f"✅ <b>DAILY BACKUP SECURED</b>\nDate: {now.strftime('%Y-%m-%d')}\nTotal Players Saved: {len(backup_data.get('players', []))}", document=doc)

        except Exception as e:
            logging.error(f"Backup Error: {e}")
            await send_sys_log(f"❌ <b>CRITICAL BACKUP FAILURE</b>\nError: <code>{e}</code>")

async def daily_bounty_task():
    global BOUNTY_TARGET_ID, BOUNTY_AMOUNT
    while True:
        # 1. Calculate wait time until Midnight IST
        now = datetime.now(IST)
        next_run = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        await asyncio.sleep((next_run - now).total_seconds())

        try:
            # 2. Identify the Target
            res = await async_db(supabase.table("players").select("id, username, balance").order("balance", desc=True).limit(1))
            if not res.data: continue
            
            king = res.data[0]
            if king['balance'] < 1000: continue # Don't bounty poor kings

            BOUNTY_TARGET_ID = king['id']
            BOUNTY_AMOUNT = int(king['balance'] * 0.10)
            king_name = html.escape(king.get('username') or "The King")

            # 3. Target King's Username with a Tag
            king_tag = f"<a href='tg://user?id={king['id']}'>@{king_name}</a>"

            msg_text = (
                "🎯 <b>DAILY BOUNTY DECLARED!</b>\n\n"
                f"The House has placed a hit on the King: <b>{king_tag}</b>\n"
                f"💰 <b>Bonus Reward:</b> {BOUNTY_AMOUNT:,} gold\n\n"
                "⚔️ The first person to successfully <b>/rob</b> them claims the gold <i>PLUS</i> this bonus!\n"
                "🛡️ <i>King, you better buy a guard...</i>"
            )

            # 4. DM-ONLY Broadcast (Fetching all players)
            start, limit = 0, 1000
            while True:
                user_res = await async_db(supabase.table("players").select("id").range(start, start + limit - 1))
                if not user_res.data: break
                
                for row in user_res.data:
                    try:
                        # Don't DM the King himself
                        if row['id'] == BOUNTY_TARGET_ID: continue
                        await bot.send_message(row['id'], msg_text, parse_mode="HTML")
                        await asyncio.sleep(0.05) # Prevent flood limits
                    except Exception: 
                        continue
                
                if len(user_res.data) < limit: break
                start += limit

        except Exception as e:
            logging.error(f"Daily Bounty Task Error: {e}")

# --- 6. DATABASE MIGRATION ---
def run_migrations():
    migration_query = """
    -- Existing Player Columns
    ALTER TABLE public.players 
    ADD COLUMN IF NOT EXISTS referred_by BIGINT,
    ADD COLUMN IF NOT EXISTS last_spin_date TEXT,
    ADD COLUMN IF NOT EXISTS has_used_ad_spin BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS total_wins INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS total_losses INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS robs_won INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS robs_lost INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_rob_time TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS last_robbed_time TIMESTAMP WITH TIME ZONE, -- 👈 ADD THIS LINE
    ADD COLUMN IF NOT EXISTS ad_spin_unlocked BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS daily_claimed_at TEXT,
    ADD COLUMN IF NOT EXISTS pity_counter INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS level INTEGER DEFAULT 1,
    ADD COLUMN IF NOT EXISTS xp BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS retro_gold_paid BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_banned BOOLEAN DEFAULT FALSE, -- 👈 ADD THIS LINE
    ADD COLUMN IF NOT EXISTS contest_score INTEGER DEFAULT 0, -- 👈 ADD THIS FOR THE MYTHIC RACE
    ADD COLUMN IF NOT EXISTS bank_balance BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_bank_interaction TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS ad_spin_unlocked BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS tasks_completed INTEGER DEFAULT 0, -- 👈 ADD THIS
    ADD COLUMN IF NOT EXISTS last_task_date TEXT,               -- 👈 ADD THIS

    -- NEW EXPLORE / PUB SYSTEM
    ADD COLUMN IF NOT EXISTS in_pub BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS pub_name TEXT,
    ADD COLUMN IF NOT EXISTS pub_game TEXT,
    ADD COLUMN IF NOT EXISTS pub_balance BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pub_plays INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_explore_time TIMESTAMP WITH TIME ZONE,
    
    -- NEW GACHA COLUMNS
    ADD COLUMN IF NOT EXISTS owned_characters INTEGER[] DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS team_1 INTEGER,
    ADD COLUMN IF NOT EXISTS team_2 INTEGER,
    ADD COLUMN IF NOT EXISTS team_3 INTEGER,
    ADD COLUMN IF NOT EXISTS last_collect_time TIMESTAMP WITH TIME ZONE,

    -- NEW PROMO CODE SYSTEM
    ADD COLUMN IF NOT EXISTS claimed_codes TEXT[] DEFAULT '{}';

    CREATE TABLE IF NOT EXISTS public.promo_codes (
        code TEXT PRIMARY KEY,
        reward_gold BIGINT DEFAULT 0,
        reward_commons INTEGER DEFAULT 0,
        max_uses INTEGER DEFAULT -1,
        uses INTEGER DEFAULT 0
    );

    -- NEW CHARACTERS TABLE
    CREATE TABLE IF NOT EXISTS public.characters (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE,  -- THIS MUST BE UNIQUE FOR UPSERT TO WORK
        anime TEXT,
        rarity TEXT,
        image TEXT
    );

    -- Add this inside your run_migrations() migration_query string:
    CREATE TABLE IF NOT EXISTS public.groups (
        id BIGINT PRIMARY KEY,
        title TEXT
    );
    """
    
    rpc_query = """
    CREATE OR REPLACE FUNCTION get_economy_stats()
    RETURNS json
    LANGUAGE plpgsql
    AS $$
    DECLARE
      total_supply bigint;
      player_count bigint;
      t_wins bigint;
      t_losses bigint;
      r_won bigint;
      r_lost bigint;
    BEGIN
      SELECT sum(balance), count(*), sum(wins), sum(total_losses), sum(robs_won), sum(robs_lost)
      INTO total_supply, player_count, t_wins, t_losses, r_won, r_lost
      FROM public.players;

      RETURN json_build_object(
        'supply', COALESCE(total_supply, 0),
        'count', COALESCE(player_count, 0),
        'wins', COALESCE(t_wins, 0),
        'losses', COALESCE(t_losses, 0),
        'robs_won', COALESCE(r_won, 0),
        'robs_lost', COALESCE(r_lost, 0)
      );
    END;
    $$;
    """
    try:
        supabase.rpc('exec_sql', {'query': migration_query}).execute()
        supabase.rpc('exec_sql', {'query': rpc_query}).execute()
    except Exception as e:
        logging.warning(f"⚠️ Migration note: {e}")

async def load_characters_to_ram():
    """Loads all characters into RAM on bot startup (Bypasses 1000 row limit)."""
    try:
        start, limit = 0, 1000
        while True:
            res = await async_db(supabase.table("characters").select("*").range(start, start + limit - 1))
            if not res.data: break
            for c in res.data:
                CHARACTER_CACHE[c['id']] = c
            if len(res.data) < limit: break
            start += limit
        logging.info(f"🌸 Loaded {len(CHARACTER_CACHE)} Waifus into RAM!")
    except Exception as e:
        logging.error(f"Failed to load characters: {e}")

# --- 7. HELPERS ---
def calculate_unclaimed_interest(p: dict) -> int:
    """Calculates the 2% daily interest based on hours passed."""
    if not p.get('bank_balance') or p['bank_balance'] <= 0:
        return 0
        
    last_interaction_str = p.get('last_bank_interaction')
    if not last_interaction_str:
        return 0
        
    last_time = datetime.fromisoformat(last_interaction_str.replace('Z', '+00:00'))
    now = datetime.now(IST)
    if last_time.tzinfo is None: last_time = last_time.replace(tzinfo=timezone.utc)

    seconds_passed = (now - last_time).total_seconds()
    hours_passed = seconds_passed / 3600.0

    # 2% daily = 0.0833% per hour
    hourly_rate = 0.02 / 24
    interest = int(p['bank_balance'] * hourly_rate * hours_passed)
    
    return max(0, interest)

def get_start_screen(user_id: int, section: str, is_private: bool, bot_username: str = None):
    """Generates the text and keyboard for the interactive /start menu."""
    if section == "main":
        text = (
            "🎰 <b>WELCOME TO MIDNIGHT CASINO</b> 🎰\n"
            "<i>Earn gold, build your empire, and dominate the vault.</i>\n\n"
            "👇 <b>Select a terminal to view your commands:</b>\n"
            "━━━━━━━━━━━━━━━━━━"
        )
        kb_rows = [
            [
                types.InlineKeyboardButton(text="🎲 Games & Gambling", callback_data=f"start_nav_games_{user_id}")
            ],
            [
                types.InlineKeyboardButton(text="🌸 Syndicate (Gacha)", callback_data=f"start_nav_gacha_{user_id}"),
                types.InlineKeyboardButton(text="🏙️ The Underworld", callback_data=f"start_nav_pub_{user_id}")
            ],
            [
                types.InlineKeyboardButton(text="🏦 Economy & Bank", callback_data=f"start_nav_econ_{user_id}"),
                types.InlineKeyboardButton(text="⚔️ PvP & Crime", callback_data=f"start_nav_pvp_{user_id}")
            ]
        ]
        
        # Web App Button Logic
        if is_private:
            kb_rows.append([types.InlineKeyboardButton(text="💰 OPEN WEB VAULT", web_app=types.WebAppInfo(url=generate_ad_link(user_id, "daily")))])
        else:
            kb_rows.append([types.InlineKeyboardButton(text="💰 OPEN WEB VAULT", url=f"https://t.me/{bot_username}?start=vault")])
            
        return text, types.InlineKeyboardMarkup(inline_keyboard=kb_rows)

    elif section == "games":
        text = (
            "🎲 <b>THE CASINO FLOOR</b> 🎲\n"
            "<i>Place your bets and beat the house.</i>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "<code>/aeroplane [amt]</code> — Smuggler's Run (Group Crash)\n"
            "<code>/roulette [amt]</code> — Russian Roulette (Group)\n"
            "<code>/poker [amt]</code> — The Standoff (Group Bluffing)\n"
            "<code>/heist [amt]</code> — The Syndicate Heist (Co-op/Betrayal)\n"
            "<code>/racing [amt]</code> — Live Track Betting (Group)\n"
            "<code>/coin [amt]</code> — Heads or Tails\n"
            "<code>/slots [amt]</code> — The Slot Machine\n"
            "<code>/dice [amt]</code> — Under/Over 7\n"
            "<code>/mines [amt] [1-24]</code> — 5x5 Minefield\n"
            "<code>/cards [amt]</code> — High or Low\n"
            "<code>/crates [3-9] [amt]</code> — Smuggler's Drop\n\n"
            "🏆 <b>SPORTS BETTING</b>\n"
            "<code>/dart [amt]</code> — Hit the Bullseye (3x)\n"
            "<code>/bowling [amt]</code> — Roll a Strike (3x)\n"
            "<code>/basket [amt]</code> — Shoot Hoops (2.5x)\n"
            "<code>/football [amt]</code> — Kick a Goal (2.5x)\n"
            "━━━━━━━━━━━━━━━━━━"
        )
	 
    elif section == "gacha":
        text = (
            "🌸 <b>SYNDICATE HQ</b> 🌸\n"
            "<i>Recruit enforcers and generate passive income.</i>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "<code>/summon</code> — Pull new fighters (500k Gold)\n"
            "<code>/harem</code> — View your character collection\n"
            "<code>/team</code> — View your active roster\n"
            "<code>/equip [id] [slot]</code> — Assign a fighter\n"
            "<code>/collect</code> — Claim passive background gold\n"
            "<code>/view [id]</code> — Inspect a specific character\n"
            "━━━━━━━━━━━━━━━━━━"
        )
    elif section == "pub":
        text = (
            "🏙️ <b>THE UNDERWORLD</b> 🏙️\n"
            "<i>Risk everything on the streets with House Money.</i>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "<code>/explore</code> — Enter a random pub for free chips\n"
            "<code>/exitpub</code> — Cash out your pub earnings\n"
            "━━━━━━━━━━━━━━━━━━"
        )
    
    elif section == "econ":
        text = (
            "🏦 <b>VAULT & ECONOMY</b> 🏦\n"
            "<i>Manage your wealth and daily tasks.</i>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "<code>/bal</code> — Check your gold & pub status\n"
            "<code>/pay</code> — Share your gold\n"
            "<code>/bank</code> — Offshore Vault & Ad Rewards\n"
            "<code>/deposit [amt]</code> — Move gold to Bank\n"
            "<code>/withdraw [amt]</code> — Move gold from Bank\n"
            "<code>/earn</code> — Complete sponsor tasks for gold\n"
            "<code>/daily</code> — Claim & double daily coins\n"
            "<code>/spin</code> — Daily lucky wheel\n"
            "<code>/stats</code> — Your Win/Loss record\n"
            "<code>/top</code> — The Global Leaderboards\n"
            "<code>/refer</code> — Invite friends & earn passive cuts\n"
            "<code>/codes</code> & <code>/redeem</code> — Promo codes\n"
            "━━━━━━━━━━━━━━━━━━"
        )

    elif section == "pvp":
        text = (
            "⚔️ <b>PVP & CRIME</b> ⚔️\n"
            "<i>Steal from the rich, duel for glory.</i>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "<code>/challenge [amt]</code> — The Main PvP Arena (9 Games)\n"
            "<code>/rob</code> — Steal gold (Reply to a user)\n"
            "<code>/bounty</code> — View the active bounty\n"
            "<code>/buy</code> — Buy Guards/Dogs to block robberies\n"
            "<code>/duel [amt]</code> — Bo3 PvP using your own Harem\n"
            "<code>/draft [amt]</code> — Bo3 PvP using a shared deck\n"
            "<code>/brawl [amt]</code> — PvP using your team\n"
            "<code>/rps [amt]</code> — Rock, Paper, Scissors wager\n"
            "━━━━━━━━━━━━━━━━━━"
        )

    # All sub-menus get a Back button
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="🔙 Back to Lobby", callback_data=f"start_nav_main_{user_id}")
    ]])
    return text, kb

@dp.callback_query(F.data.startswith("start_nav_"))
async def handle_start_nav(call: types.CallbackQuery):
    parts = call.data.split("_")
    section = parts[2]
    owner_id = int(parts[3])

    # 1. Strict User Lock
    if call.from_user.id != owner_id:
        return await call.answer("❌ This is not your terminal! Type /start to open your own.", show_alert=True)
        
    bot_me = await bot.get_me()
    is_private = call.message.chat.type == "private"
    
    text, kb = get_start_screen(owner_id, section, is_private, bot_me.username)
    
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await call.answer() # Clear loading state silently
    except Exception:
        await call.answer("Already viewing this page!", show_alert=False)

async def validate_contest_referral(referrer_id: int, recruit_id: int, recruit_name: str):
    """Fires when a recruit hits Level 2. Awards a contest point to the referrer."""
    try:
        # 1. Update RAM & DB
        if referrer_id in PLAYER_CACHE:
            PLAYER_CACHE[referrer_id]['contest_score'] = PLAYER_CACHE[referrer_id].get('contest_score', 0) + 1
            queue_db_update("players", {"contest_score": PLAYER_CACHE[referrer_id]['contest_score']}, "id", referrer_id)
        else:
            res = await async_db(supabase.table("players").select("contest_score").eq("id", referrer_id))
            if res.data:
                new_score = res.data[0].get('contest_score', 0) + 1
                await async_db(supabase.table("players").update({"contest_score": new_score}).eq("id", referrer_id))
        
        # 2. Hype Notification
        safe_name = html.escape(recruit_name or f"User_{recruit_id}")
        try:
            await bot.send_message(
                referrer_id,
                f"🎉 <b>CONTEST VALIDATION!</b>\nYour recruit <b>@{safe_name}</b> just reached Level 2!\n"
                f"📈 <b>+1 Point</b> added to your Mythic Race score! Check /contest.",
                parse_mode="HTML"
            )
        except: pass # Fails if they blocked the bot
        
        # 3. Overwatch Log
        await send_sys_log(f"🏆 <b>CONTEST POINT</b>\n<code>{referrer_id}</code> earned a point because <code>{recruit_id}</code> hit Lvl 2!")
    except Exception as e:
        logging.error(f"Contest validation error: {e}")

def make_hp_bar(hp, max_hp, length=10):
    """Generates the minimalist Style 2 HP Bar."""
    if max_hp <= 0: return "▱" * length
    filled = max(0, min(length, int(round((hp / max_hp) * length))))
    return ("▰" * filled) + ("▱" * (length - filled))

def roll_damage(base_dmg, move, is_defending):
    """Calculates final damage with +/- 15% Casino Variance and Accuracy."""
    if move == "defend": return 0, True
    
    variance = random.uniform(0.85, 1.15)
    dmg = base_dmg * variance
    hit = True
    
    if move == "heavy":
        if random.random() > 0.60: # 40% chance to miss
            return 0, False
        dmg *= 2
        
    if is_defending:
        dmg *= 0.5 # 50% Damage Reduction
        
    return int(dmg), hit

async def process_win(p: dict, win_amount: int, bet_amount: int):
    house_cut = int(win_amount * 0.05)
    player_payout = win_amount - house_cut
    
    # --- LEVEL UP SYSTEM ---
    current_xp = p.get('xp', 0)
    current_level = p.get('level', 1)
    
    new_xp = current_xp + 10
    new_level = (new_xp // 100) + 1 # Flat 100 XP per level
    
    level_up_reward = 0
    if new_level > current_level:
        level_up_reward = 1000 * (new_level - 1)
        p['level'] = new_level
        try:
            # DM the user that they leveled up!
            await bot.send_message(
                p['id'], 
                f"🎉 <b>LEVEL UP!</b>\n"
                f"Your syndicate influence has grown to <b>Level {new_level}</b>!\n"
                f"💰 <b>Reward:</b> {level_up_reward:,} gold added to your vault.", 
                parse_mode="HTML"
            )
        except:
            pass # Ignore if they blocked the bot in DMs
            
        # --- NEW: CONTEST VALIDATION TRIGGER ---
        if new_level == 2 and p.get('referred_by'):
            asyncio.create_task(validate_contest_referral(p['referred_by'], p['id'], p.get('username')))
            
    p['xp'] = new_xp
    # -----------------------
    
    updates = {
        "wins": p.get('wins', 0) + 1,
        "xp": new_xp,
        "level": new_level
    }
    
    # We add the level up reward directly to their payout balance update!
    total_balance_change = player_payout + level_up_reward
    await update_player_balance(p['id'], total_balance_change, updates)
    
    if p.get('referred_by'):
        ref_id = p['referred_by']
        ref_bonus = int(win_amount * 0.05)
        await update_player_balance(p['id'], -ref_bonus) 
        asyncio.create_task(pay_referral(ref_id, ref_bonus))

    return player_payout - (int(win_amount * 0.05) if p.get('referred_by') else 0)

async def pay_referral(ref_id, amount):
    try:
        if ref_id in PLAYER_CACHE:
            PLAYER_CACHE[ref_id]['balance'] += amount
            queue_db_update("players", {"balance": PLAYER_CACHE[ref_id]['balance']}, "id", ref_id)
        else:
            res = await async_db(supabase.table("players").select("balance").eq("id", ref_id))
            if res.data:
                new_b = res.data[0]['balance'] + amount
                await async_db(supabase.table("players").update({"balance": new_b}).eq("id", ref_id))
        await bot.send_message(ref_id, f"📈 <b>Referral Bonus!</b>\nYou earned {amount} coins from your invitee's win!", parse_mode="HTML")
    except: pass

async def get_spin_ad_kb(chat_type, user_id):
    if chat_type == "private":
        return types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="📺 Watch Ad for +1 Spin", web_app=types.WebAppInfo(url=generate_ad_link(user_id, "spin")))
        ]])
    else:
        bot_user = await bot.get_me()
        return types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="🎡 UNLOCK BONUS SPIN", url=f"https://t.me/{bot_user.username}?start=spin")
        ]])

# 1. Automatically save NEW groups the bot is added to
@dp.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=IS_NOT_MEMBER >> MEMBER))
async def bot_added_to_group(event: ChatMemberUpdated):
    if event.chat.type in ["group", "supergroup"]:
        try:
            await asyncio.to_thread(
                supabase.table("groups").upsert({"id": event.chat.id, "title": event.chat.title}).execute
            )
            logging.info(f"Bot added to new group: {event.chat.title}")
        except Exception as e:
            logging.error(f"Group save error: {e}")

# 2. Command to manually sync EXISTING groups
@dp.message(Command("sync"))
async def cmd_sync(m: types.Message):
    if m.from_user.id != 7708811819: return # Emperor only
    
    if m.chat.type in ["group", "supergroup"]:
        try:
            await asyncio.to_thread(
                supabase.table("groups").upsert({"id": m.chat.id, "title": m.chat.title}).execute
            )
            await m.answer("✅ <b>Group Synced!</b> This chat will now receive global broadcasts.", parse_mode="HTML")
        except Exception as e:
            await m.answer(f"❌ DB Error: {e}")
    else:
        await m.answer("❌ This command only works inside groups.")

# --- 8. ADMIN COMMANDS ---
@dp.message(Command("forcebounty"))
async def cmd_forcebounty(m: types.Message):
    # Security: Emperor Only
    if m.from_user.id != 7708811819:
        return await m.answer("❌ Unauthorized. Only the Emperor can declare bounties.")
        
    global BOUNTY_TARGET_ID, BOUNTY_AMOUNT
    
    status_msg = await m.answer("⏳ <b>Calculating the King's bounty...</b>", parse_mode="HTML")
    
    try:
        # 1. Identify the Target
        res = await async_db(supabase.table("players").select("id, username, balance").order("balance", desc=True).limit(1))
        if not res.data: 
            return await status_msg.edit_text("❌ The vault is empty. No players found.")
        
        king = res.data[0]
        if king['balance'] < 1000: 
            return await status_msg.edit_text("❌ The richest player is too poor to bounty right now.")

        BOUNTY_TARGET_ID = king['id']
        BOUNTY_AMOUNT = int(king['balance'] * 0.10)
        king_name = html.escape(king.get('username') or "The King")

        # 2. Prepare the Broadcast with Hyperlink Tag
        king_tag = f"<a href='tg://user?id={king['id']}'>@{king_name}</a>"
        msg_text = (
            "🎯 <b>BOUNTY DECLARED!</b>\n\n"
            f"The House has placed a hit on the King: <b>{king_tag}</b>\n"
            f"💰 <b>Bonus Reward:</b> {BOUNTY_AMOUNT:,} gold\n\n"
            "⚔️ The first person to successfully <b>/rob</b> them claims the gold <i>PLUS</i> this bonus!\n"
            "🛡️ <i>King, you better buy a guard...</i>"
        )

        # 3. DM-ONLY Broadcast
        success = 0
        start, limit = 0, 1000
        while True:
            user_res = await async_db(supabase.table("players").select("id").range(start, start + limit - 1))
            if not user_res.data: break
            for row in user_res.data:
                try:
                    await bot.send_message(row['id'], msg_text, parse_mode="HTML")
                    success += 1
                    await asyncio.sleep(0.05)
                except Exception: continue
            if len(user_res.data) < limit: break
            start += limit

        await status_msg.edit_text(f"✅ <b>Bounty DMs sent to {success} users!</b>", parse_mode="HTML")

    except Exception as e:
        await status_msg.edit_text(f"❌ <b>Error:</b> {e}", parse_mode="HTML")

@dp.message(Command("addcode"))
async def cmd_addcode(m: types.Message):
    if m.from_user.id != 7708811819: return await m.answer("❌ Unauthorized.")
    
    args = m.text.split()
    if len(args) < 5:
        return await m.answer(
            "🛠️ <b>How to create a code:</b>\n"
            "`/addcode [CODE] [GOLD] [COMMONS] [MAX_USES]`\n\n"
            "<i>Examples:</i>\n"
            "`/addcode STARTERPACK 0 3 -1` (Infinite uses, gives 3 Commons)\n"
            "`/addcode FREEMONEY 100000 0 50` (First 50 people get 100k gold)",
            parse_mode="HTML"
        )
        
    try:
        code = args[1].lower()
        gold = int(args[2])
        commons = int(args[3])
        max_uses = int(args[4])
        
        await async_db(supabase.table("promo_codes").upsert({
            "code": code,
            "reward_gold": gold,
            "reward_commons": commons,
            "max_uses": max_uses,
            "uses": 0
        }))
        
        await m.answer(f"✅ <b>Promo Code Active!</b>\nCode: <code>{code.upper()}</code>\nRewards: {gold:,} Gold | {commons} Commons\nMax Uses: {'Infinite' if max_uses == -1 else max_uses}", parse_mode="HTML")
    except Exception as e:
        await m.answer(f"❌ Error: {e}")

@dp.message(Command("delcode"))
async def cmd_delcode(m: types.Message):
    if m.from_user.id != 7708811819: return
    args = m.text.split()
    if len(args) < 2: return await m.answer("❌ Usage: `/delcode [code]`", parse_mode="Markdown")
    
    await async_db(supabase.table("promo_codes").delete().eq("code", args[1].lower()))
    await m.answer(f"🗑️ Code <b>{args[1].upper()}</b> has been deleted.", parse_mode="HTML")

@dp.message(Command("broadcast"))
async def cmd_broadcast(m: types.Message):
    if m.from_user.id != 7708811819:
        return await m.answer("❌ Unauthorized.")

    target_msg = m.reply_to_message
    text_to_send = m.text.replace("/broadcast", "").strip()

    if not target_msg and not text_to_send:
        return await m.answer(
            "❌ <b>How to use:</b>\n"
            "1. Reply to any message/image/video with `/broadcast`\n"
            "2. OR type `/broadcast [your message]`", 
            parse_mode="HTML"
        )

    status_msg = await m.answer("⏳ <b>Fetching players and groups from the database...</b>", parse_mode="HTML")

    all_targets = []
    
    # 1. Fetch Players (DMs)
    try:
        start, limit = 0, 1000
        while True:
            res = await async_db(supabase.table("players").select("id").range(start, start + limit - 1))
            if not res.data: break
            for row in res.data: all_targets.append(row['id'])
            if len(res.data) < limit: break
            start += limit
    except Exception as e:
        return await status_msg.edit_text(f"❌ Player DB Error: {e}")

    # 2. Fetch Groups
    try:
        group_res = await async_db(supabase.table("groups").select("id"))
        if group_res.data:
            for row in group_res.data:
                # Ensure we don't duplicate if a group somehow ended up in the players table
                if row['id'] not in all_targets: 
                    all_targets.append(row['id'])
    except Exception as e:
        return await status_msg.edit_text(f"❌ Group DB Error: {e}")

    if not all_targets:
        return await status_msg.edit_text("❌ No users or groups found to broadcast to.")

    await status_msg.edit_text(
        f"🚀 <b>Broadcasting to {len(all_targets)} targets (Players + Groups)...</b>\n"
        f"<i>This will take some time to avoid Telegram's spam limits.</i>", 
        parse_mode="HTML"
    )

    success = 0
    failed = 0

    for target_id in all_targets:
        try:
            if target_msg:
                await target_msg.send_copy(chat_id=target_id)
            else:
                await bot.send_message(chat_id=target_id, text=text_to_send, parse_mode="HTML")
            success += 1
        except TelegramRetryAfter as e:
            # OBEY TELEGRAM! Sleep for however long they demand, then retry.
            logging.warning(f"Rate limited by Telegram. Sleeping for {e.retry_after} seconds.")
            await asyncio.sleep(e.retry_after)
            # Retry sending to this same user
            try:
                if target_msg: await target_msg.send_copy(chat_id=target_id)
                else: await bot.send_message(chat_id=target_id, text=text_to_send, parse_mode="HTML")
                success += 1
            except Exception: failed += 1
        except Exception:
            # Normal failure (user blocked the bot)
            failed += 1

    await m.answer(
        f"✅ <b>Broadcast Complete!</b>\n\n"
        f"🎯 <b>Delivered:</b> {success}\n"
        f"❌ <b>Blocked/Failed:</b> {failed}", 
        parse_mode="HTML"
    )

@dp.message(Command("addchar"))
async def cmd_addchar(m: types.Message, command: CommandObject):
    # Security: Emperor Only
    if m.from_user.id != 7708811819: return

    args = command.args
    if not args:
        return await m.answer(
            "❌ <b>Format:</b>\n`/addchar Name | Anime | Rarity | [Optional URL]`\n\n"
            "<i>Example 1 (Auto Fetch):</i> `/addchar Toji Fushiguro | Jujutsu Kaisen | Mythic`\n"
            "<i>Example 2 (Manual HD Image):</i> `/addchar Toji | JJK | Mythic | https://my-hd-link.com/toji.jpg`", 
            parse_mode="HTML"
        )
    
    try:
        parts = [p.strip() for p in args.split("|")]
        if len(parts) < 3:
            return await m.answer("❌ <b>Format Error!</b> You need at least: Name | Anime | Rarity", parse_mode="HTML")
        
        name, anime, rarity = parts[0], parts[1], parts[2].capitalize()
        
        if rarity not in RARITY_RATES:
            return await m.answer(f"❌ <b>Invalid rarity!</b> Must be Common, Rare, Epic, Legendary, or Mythic.", parse_mode="HTML")
            
        status_msg = await m.answer("⏳ <i>Processing character data...</i>", parse_mode="HTML")

        image_url = None

        # --- 🌟 THE FIX: MANUAL OVERRIDE CHECK ---
        # If you provided a 4th part that looks like a link, use it!
        if len(parts) >= 4 and parts[3].startswith("http"):
            image_url = parts[3]
        else:
            # --- 🌐 AUTOMATIC IMAGE FETCHING (Improved Accuracy) ---
            # We now search the Name AND the Anime together so it doesn't grab the wrong person
            safe_query = urllib.parse.quote(f"{name} {anime}")
            api_url = f"https://api.jikan.moe/v4/characters?q={safe_query}&limit=1"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("data") and len(data["data"]) > 0:
                            # Try to grab the high-quality webp first, fallback to jpg
                            images = data["data"][0]["images"]
                            image_url = images.get("webp", {}).get("image_url") or images.get("jpg", {}).get("image_url")
        
        # Fallback if MAL doesn't find the character and no manual link was provided
        if not image_url:
            return await status_msg.edit_text(f"❌ <b>Search Failed!</b>\nCould not find '{name}' automatically. Please use the manual URL override.", parse_mode="HTML")

        # --- 💾 DATABASE UPSERT LOGIC ---
        res = await async_db(supabase.table("characters").upsert({
            "name": name, 
            "anime": anime, 
            "rarity": rarity, 
            "image": image_url
        }, on_conflict="name")) 
        
        if not res.data:
            return await status_msg.edit_text("❌ Database error during save.")

        # Update RAM Cache
        new_char = res.data[0]
        CHARACTER_CACHE[new_char['id']] = new_char
        
        msg = "Character Added!" if len(res.data) > 0 else "Character Updated!"
        
        await status_msg.delete()
        await m.answer_photo(
            photo=image_url, 
            caption=(
                f"✅ <b>{msg}</b>\n\n"
                f"🌸 <b>Name:</b> {name}\n"
                f"📺 <b>Anime:</b> {anime}\n"
                f"✨ <b>Rarity:</b> {rarity}\n"
                f"💰 <b>Passive:</b> {RARITY_RATES[rarity]:,}/hr"
            ), 
            parse_mode="HTML"
        )
        
    except Exception as e:
        logging.error(f"Addchar error: {e}")
        try:
            await status_msg.edit_text(f"❌ <b>Error:</b>\n{str(e)}", parse_mode="HTML")
        except:
            await m.answer(f"❌ <b>Error:</b>\n{str(e)}", parse_mode="HTML")


@dp.message(Command("resetall"))
async def cmd_resetall(m: types.Message):
    if m.from_user.id != 7708811819:
        return await m.answer("❌ You do not have Emperor privileges.")

    kb = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="🚨 CONFIRM WIPE", callback_data="confirm_reset_all"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_reset_all")
    ]])
    await m.answer(
        "⚠️ <b>DANGER ZONE: NUCLEAR WIPE</b> ⚠️\n\n"
        "This will permanently delete <b>ALL</b> player data, balances, and stats.\n"
        "Are you absolutely sure?", 
        reply_markup=kb, parse_mode="HTML"
    )

@dp.callback_query(F.data == "confirm_reset_all")
async def handle_confirm_reset(call: types.CallbackQuery):
    if call.from_user.id != 7708811819:
        return await call.answer("❌ Unauthorized.", show_alert=True)
    res = await async_db(supabase.table("players").delete().gt("id", 0))
    PLAYER_CACHE.clear()
    await call.message.edit_text(f"✅ <b>DATABASE WIPED.</b>\nDeleted {len(res.data)} player records.", parse_mode="HTML")

@dp.callback_query(F.data == "cancel_reset_all")
async def handle_cancel_reset(call: types.CallbackQuery):
    await call.message.edit_text("✅ Reset cancelled. The empire is safe.")

@dp.message(Command("ban"))
async def cmd_ban(m: types.Message):
    if m.from_user.id != 7708811819: return # Emperor Only
    
    args = m.text.split()
    target_id = None
    
    # Allow banning by replying to a message OR typing their ID
    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
    elif len(args) > 1 and args[1].isdigit():
        target_id = int(args[1])
    else:
        return await m.answer("❌ <b>Usage:</b> Reply to a user with <code>/ban</code> or use <code>/ban [user_id]</code>", parse_mode="HTML")
        
    if target_id == 7708811819:
        return await m.answer("❌ You cannot ban the Emperor.")
        
    try:
        # 1. Update Database
        await async_db(supabase.table("players").update({"is_banned": True}).eq("id", target_id))
        
        # 2. Kick them out of RAM & Active Games instantly
        if target_id in PLAYER_CACHE:
            del PLAYER_CACHE[target_id]
        active_games.discard(target_id)
        
        # 3. Fire the Overwatch Log
        asyncio.create_task(send_log(f"🔨 <b>BAN HAMMER</b>\nUser <code>{target_id}</code> has been permanently exiled from the Syndicate."))
        
        await m.answer(f"✅ <b>Target Neutralized.</b>\nUser <code>{target_id}</code> has been permanently banned.", parse_mode="HTML")
        
        # 4. Attempt to DM them the bad news
        try:
            await bot.send_message(target_id, "⛔ <b>YOU HAVE BEEN BANNED</b>\n\nThe Syndicate has exiled you for violating the rules. You can no longer interact with this casino.", parse_mode="HTML")
        except: pass
        
    except Exception as e:
        await m.answer(f"❌ Error: {e}")

@dp.message(Command("unban"))
async def cmd_unban(m: types.Message):
    if m.from_user.id != 7708811819: return # Emperor Only
    
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit():
        return await m.answer("❌ <b>Usage:</b> <code>/unban [user_id]</code>", parse_mode="HTML")
        
    target_id = int(args[1])
    
    try:
        # 1. Lift the ban in the database
        await async_db(supabase.table("players").update({"is_banned": False}).eq("id", target_id))
        
        # 2. Clear cache so their profile reloads properly on next command
        if target_id in PLAYER_CACHE: 
            del PLAYER_CACHE[target_id]
        
        asyncio.create_task(send_log(f"🕊️ <b>UNBANNED</b>\nUser <code>{target_id}</code> has been allowed back into the Syndicate."))
        await m.answer(f"✅ <b>Ban Lifted.</b>\nUser <code>{target_id}</code> has been pardoned.", parse_mode="HTML")
        
        try:
            await bot.send_message(target_id, "🕊️ <b>YOU HAVE BEEN PARDONED</b>\n\nThe Syndicate has lifted your ban. Welcome back to the tables.", parse_mode="HTML")
        except: pass
        
    except Exception as e:
        await m.answer(f"❌ Error: {e}")


@dp.message(Command("give"))
async def cmd_give(m: types.Message):
    if m.from_user.id != 7708811819: return # Emperor Only
    args = m.text.split()
    
    if len(args) < 4: 
        return await m.answer(
            "❌ <b>Admin Usage:</b>\n"
            "💰 Give Gold: <code>/give [user_id] gold [amount]</code>\n"
            "🌸 Give Char: <code>/give [user_id] char [char_id]</code>",
            parse_mode="HTML"
        )
    
    try:
        target_id = int(args[1])
        give_type = args[2].lower()
        value = int(args[3])
    except ValueError:
        return await m.answer("❌ The user_id and amount/char_id must be numbers.")
        
    try:
        # Safely fetch the target player (even if they are offline)
        temp_user = types.User(id=target_id, is_bot=False, first_name="Unknown")
        p = await get_player_fast(temp_user)
        
        if not p:
            return await m.answer("❌ User not found in database.")

        if give_type == "gold":
            await update_player_balance(target_id, value)
            new_bal = p['balance'] + value # Just for the admin message
            await m.answer(f"✅ <b>Funds Transferred!</b>\nAdded <b>{value:,} gold</b> to User {target_id}.\nNew Balance: {new_bal:,}", parse_mode="HTML")
            
            try:
                await bot.send_message(target_id, f"🛠️ <b>Admin Support</b>\n<b>{value:,} gold</b> has been manually deposited into your vault to compensate for the lost game.", parse_mode="HTML")
            except: pass

        elif give_type == "char":
            if value not in CHARACTER_CACHE:
                return await m.answer(f"❌ Character ID <code>{value}</code> does not exist in the database.", parse_mode="HTML")
                
            char = CHARACTER_CACHE[value]
            owned = p.get('owned_characters') or []
            
            if value in owned:
                return await m.answer(f"⚠️ User {target_id} already owns <b>{char['name']}</b>.", parse_mode="HTML")
                
            owned.append(value)
            updates = {"owned_characters": owned}
            
            # Start passive generation if it's their very first character
            if not p.get('last_collect_time'):
                updates['last_collect_time'] = datetime.now(IST).isoformat()
                
            # Use your existing updater to safely push this to RAM and Supabase
            await update_player_balance(target_id, 0, updates)
            p['owned_characters'] = owned
            
            await m.answer(f"✅ <b>Character Granted!</b>\nAdded <b>{char['name']}</b> (ID: {value}) to User {target_id}'s collection.", parse_mode="HTML")
            
            try:
                e = EMOJIS.get(char['rarity'], "✨")
                await bot.send_photo(
                    target_id,
                    photo=char['image'],
                    caption=f"🛠️ <b>Admin Support</b>\n<b>{char['name']}</b> {e} has been manually restored to your collection!",
                    parse_mode="HTML"
                )
            except: pass

        else:
            await m.answer("❌ Invalid type. Use <code>gold</code> or <code>char</code>.", parse_mode="HTML")
            
    except Exception as e:
        await m.answer(f"❌ Error: {e}")

@dp.message(Command("cancel"))
async def cmd_cancel(m: types.Message):
    if m.from_user.id != 7708811819: return
    args = m.text.split()
    if len(args) < 2: return await m.answer("❌ Usage: /cancel [user_id]")
    
    try:
        target_id = int(args[1])
    except ValueError:
        return await m.answer("❌ user_id must be a number.")
        
    refund = 0
    game_type = "Unknown"
    defender_id = None

    if target_id in coin_games:
        refund = coin_games.pop(target_id)['bet']
        game_type = "Coin Flip"
    elif target_id in dice_games:
        refund = dice_games.pop(target_id)['bet']
        game_type = "Dice"
    elif target_id in card_games:
        refund = card_games.pop(target_id)['bet']
        game_type = "Cards"
    elif target_id in mine_games:
        refund = mine_games.pop(target_id)['bet']
        game_type = "Mines"
    elif target_id in crate_games:
        refund = crate_games.pop(target_id)['bet']
        game_type = "Crates"
    elif target_id in rps_games:
        refund = rps_games.pop(target_id)['bet']
        game_type = "RPS"
    elif target_id in duel_games:
        refund = duel_games.pop(target_id)['bet']
        game_type = "Duel"
    elif target_id in draft_games:
        refund = draft_games.pop(target_id)['bet']
        game_type = "Draft"
    elif target_id in brawl_games:
        game = brawl_games.pop(target_id)
        refund = game['bet']
        game_type = "Brawl"
        if game.get('status') == 'active':
            defender_id = game.get('defender')
    elif target_id in challenge_games:
        game = challenge_games.pop(target_id)
        game_type = "PvP Challenge"
        if game.get('status') == 'playing':
            refund = game['bet']
            defender_id = game.get('defender')
        else:
            refund = 0 # No escrow was taken yet during the selection phase

    # Force clear the target from the active lock
    active_games.discard(target_id)

    # 1. Handle Host / Solo Refund
    if refund > 0:
        if target_id in PLAYER_CACHE:
            await update_player_balance(target_id, refund)
        else:
            res = await async_db(supabase.table("players").select("balance").eq("id", target_id))
            if res.data:
                target_bal = res.data[0]['balance']
                await async_db(supabase.table("players").update({"balance": target_bal + refund}).eq("id", target_id))
                
        await m.answer(f"✅ <b>Game Cancelled</b>\nUser: {target_id}\nGame: {game_type}\nRefund: {refund}", parse_mode="HTML")
        try:
            await bot.send_message(target_id, f"🛠️ <b>Admin Support</b>\nYour stuck {game_type} game was cancelled. <b>{refund} gold</b> has been refunded.", parse_mode="HTML")
        except: pass
    elif game_type != "Unknown":
        await m.answer(f"✅ <b>Game Cancelled</b>\nUser: {target_id}\nGame: {game_type}\n<i>No escrow to refund.</i>", parse_mode="HTML")
    else:
        await m.answer(f"❌ User {target_id} has no stuck games as the host.", parse_mode="HTML")

    # 2. Handle Defender Refund (If stuck mid-PvP)
    if defender_id:
        active_games.discard(defender_id)
        if defender_id in PLAYER_CACHE:
            await update_player_balance(defender_id, refund)
        else:
            res = await async_db(supabase.table("players").select("balance").eq("id", defender_id))
            if res.data:
                def_bal = res.data[0]['balance']
                await async_db(supabase.table("players").update({"balance": def_bal + refund}).eq("id", defender_id))
        try:
            await bot.send_message(defender_id, f"🛠️ <b>Admin Support</b>\nThe {game_type} match you were in got stuck and was cancelled. <b>{refund} gold</b> has been refunded.", parse_mode="HTML")
        except: pass

@dp.message(Command("economy"))
async def cmd_economy(m: types.Message):
    if m.from_user.id != 7708811819: 
        return await m.answer("❌ Unauthorized.")

    msg = await m.answer("⏳ <b>Calculating economy stats...</b>", parse_mode="HTML")

    try:
        res = await async_db(supabase.rpc("get_economy_stats"))
        data = res.data
        
        if not data:
            return await msg.edit_text("📉 Database is empty.")
            
        supply = data.get('supply', 0)
        count = data.get('count', 0)
        wins = data.get('wins', 0)
        losses = data.get('losses', 0)
        
        ratio = round(wins / losses, 2) if losses > 0 else 0
        avg_bal = int(supply / count) if count > 0 else 0

        text = (
            f"💰 <b>MIDNIGHT ECONOMY REPORT</b> 💰\n"
            f"———————————————————\n"
            f"🏦 <b>Total Gold Supply:</b> {supply:,}\n"
            f"👥 <b>Total Players:</b> {count:,}\n"
            f"🌊 <b>Avg. Balance:</b> {avg_bal:,}\n\n"
            f"📊 <b>Game Activity:</b>\n"
            f"— Total Wins: {wins:,}\n"
            f"— Total Losses: {losses:,}\n"
            f"— Win/Loss Ratio: {ratio}\n\n"
            f"🥷 <b>Crime Stats:</b>\n"
            f"— Robberies Success: {data.get('robs_won', 0):,}\n"
            f"— Robberies Failed: {data.get('robs_lost', 0):,}\n"
        )
        await msg.edit_text(text, parse_mode="HTML")
    except Exception as e:
        await msg.edit_text(f"❌ Error calculating economy: {e}")

# --- 9. PARTNER COMMANDS ---
@dp.message(Command("authorize"))
async def cmd_authorize(m: types.Message):
    if m.from_user.id != 7708811819: return await m.answer("❌ Unauthorized. Only the Emperor can grant power.")
    if not m.reply_to_message: return await m.answer("❌ Reply to the person you want to authorize.")

    target = m.reply_to_message.from_user
    await async_db(supabase.table("authorized_partners").upsert({
        "owner_id": target.id,
        "owner_name": target.username or target.first_name
    }))
    await m.answer(f"✅ <b>{target.first_name}</b> is now an Authorized Partner!", parse_mode="HTML")

@dp.message(Command("take"))
async def cmd_take(m: types.Message):
    # Verify Partner
    res = await async_db(supabase.table("authorized_partners").select("*").eq("owner_id", m.from_user.id))
    if not res.data: return await m.answer("❌ You are not an authorized partner.")
    if not m.reply_to_message: return await m.answer("❌ Reply to the user you want to take coins from.")

    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit(): return await m.answer("❌ Usage: /take [amount]")

    amount = int(args[1])
    if amount <= 0: return await m.answer("❌ Amount must be greater than 0.")
    
    target = m.reply_to_message.from_user
    p = await get_player_fast(target)
    if p['balance'] < amount: return await m.answer("❌ User does not have enough gold.")

    kb = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="✅ Confirm Payment", callback_data=f"confirm_take:{m.from_user.id}:{target.id}:{amount}"),
        types.InlineKeyboardButton(text="❌ Cancel", callback_data=f"cancel_take:{target.id}")
    ]])
    await m.answer(
        f"⚠️ <b>PAYMENT REQUEST</b>\n\n"
        f"Partner @{m.from_user.username} wants to take <b>{amount:,} gold</b> from you.\n\n"
        f"<i>Click confirm to pay. This cannot be undone.</i>",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("confirm_take:"))
async def handle_confirm_take(call: types.CallbackQuery):
    _, partner_id, target_id, amount = call.data.split(":")
    if call.from_user.id != int(target_id):
        return await call.answer("❌ This payment request is not for you!", show_alert=True)
        
    amount = int(amount)
    partner_id = int(partner_id)
    
    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            return await call.answer("❌ Request already processed!", show_alert=True)

        p = await get_player_fast(call.from_user)
        if p['balance'] < amount: return await call.answer("❌ Insufficient gold!", show_alert=True)

        await update_player_balance(call.from_user.id, -amount)
        
        await call.message.edit_text(
            f"✅ <b>Payment Successful!</b>\n\n"
            f"Sent <b>{amount:,} gold</b> to Partner ID: <code>{partner_id}</code>.\n"
            f"They will now provide your resources.", parse_mode="HTML"
        )
        
        try:
            safe_name = html.escape(call.from_user.first_name)
            await bot.send_message(partner_id, f"💰 <b>Payment Received!</b>\nUser {safe_name} confirmed the payment of {amount:,} gold.", parse_mode="HTML")
        except Exception as e: 
            logging.warning(f"Notification error: {e}")
    finally:
        active_games.discard(call.from_user.id)

@dp.callback_query(F.data.startswith("cancel_take:"))
async def handle_cancel_take(call: types.CallbackQuery):
    target_id = int(call.data.split(":")[1])
    if call.from_user.id != target_id: return await call.answer("❌ You cannot cancel this request!", show_alert=True)
    
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await call.message.edit_text("❌ Payment cancelled by the user.")

@dp.message(Command("partners"))
async def cmd_partners(m: types.Message):
    res = await async_db(supabase.table("authorized_partners").select("*"))
    if not res.data: return await m.answer("🏢 <b>Midnight Business Bureau</b>\n\nNo partners are currently authorized. Check back soon!", parse_mode="HTML")

    partner_list = ""
    for p in res.data:
        partner_list += f"— @{p['owner_name']}\n"

    text = (
        "🤝 <b>OFFICIAL PARTNERS</b>\n\n"
        "The following bot owners are authorized to accept Midnight Gold in exchange for their resources:\n\n"
        f"{partner_list}\n"
        "⚠️ <i>Never confirm a /take request unless you are sure of the trade!</i>"
    )
    await m.answer(text, parse_mode="HTML")

# --- 10. ROCK PAPER SCISSORS (Full) ---
@dp.message(Command("rps"))
async def cmd_rps(m: types.Message):
    if not m.reply_to_message:
        return await m.answer("❌ Reply to the user you want to challenge!")
    
    target = m.reply_to_message.from_user
    if target.id == m.from_user.id:
        return await m.answer("❌ You cannot challenge yourself.")
    if target.is_bot:
        return await m.answer("❌ You cannot challenge a bot.")

    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit():
        return await m.answer("❌ Usage: /rps [amount]")
    
    amount = int(args[1])
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    if m.from_user.id in rps_games: return await m.answer("❌ You already have a pending challenge!")
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")

    active_games.add(m.from_user.id)
    try:
        p_challenger = await get_player_fast(m.from_user)
        if amount > p_challenger['balance']: return await m.answer("❌ Insufficient balance.")

        p_defender = await get_player_fast(target)
        if amount > p_defender['balance']: return await m.answer("❌ Opponent does not have enough gold.")

        await update_player_balance(m.from_user.id, -amount)

        rps_games[m.from_user.id] = {
            "challenger": m.from_user.id,
            "defender": target.id,
            "bet": amount,
            "round": 1,
            "scores": {m.from_user.id: 0, target.id: 0},
            "moves": {},
            "names": {m.from_user.id: m.from_user.first_name, target.id: target.first_name},
            "ts": datetime.now().timestamp()
        }

        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="Accept Brawl 🥊", callback_data=f"rps_accept_{m.from_user.id}"),
            types.InlineKeyboardButton(text="Walk Away ❌", callback_data=f"rps_decline_{m.from_user.id}")
        ]])

        safe_c = html.escape(m.from_user.first_name)
        safe_d = html.escape(target.first_name)
        link_c = f"<a href='tg://openmessage?user_id={m.from_user.id}'>@{safe_c}</a>"
        link_d = f"<a href='tg://openmessage?user_id={target.id}'>@{safe_d}</a>"
        
        await m.answer(
            f"🥊 <b>𝗨𝗡𝗗𝗘𝗥𝗚𝗥𝗢𝗨𝗡𝗗 𝗕𝗥𝗔𝗪𝗟</b> 🥊\n"
            f"<i>A street challenge has been issued.</i>\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"🩸 <b>Instigator:</b> <b>{link_c}</b>\n"
            f"🎯 <b>Target:</b> <b>{link_d}</b>\n\n"
            f"💰 <b>Wager:</b> {amount:,} Gold\n"
            f"🏆 <b>Total Pot:</b> {amount * 2:,} Gold\n\n"
            f"<b>[ 𝗧𝗛𝗘 𝗧𝗘𝗥𝗠𝗦 ]</b>\n"
            f"• Classic Rock, Paper, Scissors.\n"
            f"• First to 3 rounds takes the pot.\n\n"
            f"<i>Will you step into the ring, {link_d}?</i>",
            reply_markup=kb, parse_mode="HTML"
        )

    finally:
        active_games.discard(m.from_user.id)

@dp.callback_query(F.data.startswith("rps_accept_"))
async def handle_rps_accept(call: types.CallbackQuery):
    challenger_id = int(call.data.split("_")[2])
    game = rps_games.get(challenger_id)
    
    if not game: return await call.answer("❌ Challenge expired.", show_alert=True)
    if call.from_user.id != game['defender']: return await call.answer("❌ This challenge is not for you!", show_alert=True)

    if call.from_user.id in active_games: return await call.answer("⏳ Processing...")
    active_games.add(call.from_user.id)
    
    try:
        p_defender = await get_player_fast(call.from_user)
        if game['bet'] > p_defender['balance']:
            await update_player_balance(challenger_id, game['bet'])
            del rps_games[challenger_id]
            return await call.message.edit_text("❌ Opponent ran out of money! Challenge cancelled.")

        await update_player_balance(call.from_user.id, -game['bet'])
        game['ts'] = datetime.now().timestamp()
        
        await call.message.edit_text(
            f"🥊 <b>Round 1/3</b>\nMake your moves!",
            reply_markup=get_rps_keyboard(challenger_id),
            parse_mode="HTML"
        )
    finally:
        active_games.discard(call.from_user.id)

@dp.callback_query(F.data.startswith("rps_decline_"))
async def handle_rps_decline(call: types.CallbackQuery):
    challenger_id = int(call.data.split("_")[2])
    game = rps_games.get(challenger_id)
    
    if not game: return await call.answer("❌ Challenge expired.", show_alert=True)
    if call.from_user.id != game['defender'] and call.from_user.id != game['challenger']:
        return await call.answer("❌ Not your game.", show_alert=True)

    await update_player_balance(challenger_id, game['bet'])
    del rps_games[challenger_id]
    await call.message.edit_text("❌ Challenge declined/cancelled. Gold refunded.")

def get_rps_keyboard(game_id):
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [
            types.InlineKeyboardButton(text="🪨 Rock", callback_data=f"rps_move_{game_id}_rock"),
            types.InlineKeyboardButton(text="📄 Paper", callback_data=f"rps_move_{game_id}_paper"),
            types.InlineKeyboardButton(text="✂️ Scissors", callback_data=f"rps_move_{game_id}_scissor")
        ]
    ])

@dp.callback_query(F.data.startswith("rps_move_"))
async def handle_rps_move(call: types.CallbackQuery):
    _, _, game_id, move = call.data.split("_")
    game_id = int(game_id)
    game = rps_games.get(game_id)

    if not game: return await call.answer("❌ Game ended.", show_alert=True)
    if call.from_user.id not in [game['challenger'], game['defender']]:
        return await call.answer("❌ Not your game!", show_alert=True)
    
    if call.from_user.id in game['moves']:
        return await call.answer("⏳ Waiting for opponent...", show_alert=True)

    game['moves'][call.from_user.id] = move
    game['ts'] = datetime.now().timestamp()
    
    p1_status = "✅" if game['moves'].get(game['challenger']) else "thinking..."
    p2_status = "✅" if game['moves'].get(game['defender']) else "thinking..."
    
    if len(game['moves']) < 2:
        await call.message.edit_text(
            f"🥊 <b>Round {game['round']}/3</b>\n\n"
            f"👤 {game['names'][game['challenger']]}: {p1_status}\n"
            f"👤 {game['names'][game['defender']]}: {p2_status}",
            reply_markup=get_rps_keyboard(game_id),
            parse_mode="HTML"
        )
        return

    m1 = game['moves'][game['challenger']]
    m2 = game['moves'][game['defender']]
    
    winner_id = None
    if m1 == m2: pass 
    elif (m1=="rock" and m2=="scissor") or (m1=="paper" and m2=="rock") or (m1=="scissor" and m2=="paper"):
        winner_id = game['challenger']
    else:
        winner_id = game['defender']

    if winner_id: game['scores'][winner_id] += 1
    
    round_res = "🤝 Draw!"
    if winner_id: round_res = f"🏆 {game['names'][winner_id]} wins round!"
    
    text = (
        f"🥊 <b>Round {game['round']} Results</b>\n"
        f"{game['names'][game['challenger']]}: {m1}\n"
        f"{game['names'][game['defender']]}: {m2}\n\n"
        f"<b>{round_res}</b>\n"
        f"Score: {game['scores'][game['challenger']]} - {game['scores'][game['defender']]}"
    )

    if game['round'] < 3:
        game['round'] += 1
        game['moves'] = {}
        await call.message.edit_text(text + "\n\n👇 <b>Next Round!</b>", reply_markup=get_rps_keyboard(game_id), parse_mode="HTML")
    else:
        s1 = game['scores'][game['challenger']]
        s2 = game['scores'][game['defender']]
        
        final_winner = None
        if s1 > s2: final_winner = game['challenger']
        elif s2 > s1: final_winner = game['defender']
        
        if final_winner:
            pot = int(game['bet'] * 2 * 0.95) 
            p_win = await get_player_fast(types.User(id=final_winner, is_bot=False, first_name=""))
            await update_player_balance(final_winner, pot, {"wins": p_win.get('wins', 0) + 1})
            await call.message.edit_text(f"{text}\n\n👑 <b>FINAL WINNER: {game['names'][final_winner]}</b>\nWon {pot:,} gold!", parse_mode="HTML")
            
            # --- ADD THE AUDIT LOG HERE ---
            asyncio.create_task(send_log(f"🥊 <b>RPS WIN</b>\n<a href='tg://user?id={final_winner}'>{game['names'][final_winner]}</a> beat their opponent and took a <b>{pot:,} Gold</b> pot!"))
        else:
            await update_player_balance(game['challenger'], game['bet'])
            await update_player_balance(game['defender'], game['bet'])
            await call.message.edit_text(f"{text}\n\n🤝 <b>GAME DRAW!</b>\nGold refunded to both.", parse_mode="HTML")
        
        del rps_games[game_id]

@dp.message(Command("brawl"))
async def cmd_brawl(m: types.Message):
    if not m.reply_to_message: return await m.answer("❌ You must reply to the user you want to brawl!")
    target = m.reply_to_message.from_user
    if target.id == m.from_user.id or target.is_bot: return await m.answer("❌ Invalid target.")

    args = m.text.split()
    amount_str = next((arg for arg in args[1:] if arg.isdigit()), None)
    if not amount_str: return await m.answer("❌ <b>Usage:</b> `/brawl [amount]` (Must reply to a user)", parse_mode="HTML")
        
    amount = int(amount_str)
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    if m.from_user.id in brawl_games: return await m.answer("❌ You already have a pending brawl!")
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")

    active_games.add(m.from_user.id)
    try:
        p_c = await get_player_fast(m.from_user)
        p_d = await get_player_fast(target)
        
        if amount > p_c['balance']: return await m.answer("❌ Insufficient balance.")
        if amount > p_d['balance']: return await m.answer("❌ Opponent does not have enough gold.")
        
        # Check if both players have a full 3-man team equipped
        c_team = [p_c.get('team_1'), p_c.get('team_2'), p_c.get('team_3')]
        d_team = [p_d.get('team_1'), p_d.get('team_2'), p_d.get('team_3')]
        
        if not all(c_team): return await m.answer("❌ You must equip 3 characters to your /team to enter the Brawl Arena!")
        if not all(d_team): return await m.answer(f"❌ @{target.first_name} does not have a full /team equipped!")

        await update_player_balance(m.from_user.id, -amount) # Escrow

        brawl_games[m.from_user.id] = {
            "challenger": m.from_user.id, "defender": target.id, "bet": amount,
            "c_team_ids": c_team, "d_team_ids": d_team
        }

        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="⚔️ Accept Brawl", callback_data=f"brawl_accept_{m.from_user.id}"),
            types.InlineKeyboardButton(text="❌ Walk Away", callback_data=f"brawl_decline_{m.from_user.id}")
        ]])
        
        safe_c = html.escape(m.from_user.first_name)
        safe_d = html.escape(target.first_name)
        link_c = f"<a href='tg://openmessage?user_id={m.from_user.id}'>{safe_c}</a>"
        link_d = f"<a href='tg://openmessage?user_id={target.id}'>{safe_d}</a>"
        
        msg = await m.answer(
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⚔️ <b>𝗦𝗬𝗡𝗗𝗜𝗖𝗔𝗧𝗘 𝗕𝗥𝗔𝗪𝗟 (𝟯𝘃𝟯)</b> ⚔️\n"
            f"<i>A bloody street fight for gold.</i>\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"🩸 <b>Instigator:</b> <b>{link_c}</b>\n"
            f"🎯 <b>Target:</b> <b>{link_d}</b>\n\n"
            f"💰 <b>Wager:</b> {amount:,} Gold\n"
            f"🏆 <b>Total Pot:</b> {amount * 2:,} Gold\n\n"
            f"<b>[ 𝗧𝗛𝗘 𝗥𝗨𝗟𝗘𝗦 ]</b>\n"
            f"• 3v3 Simultaneous Combat.\n"
            f"• Lock in your moves blindly.\n"
            f"• Faster rarity strikes first!\n\n"
            f"<i>Draw your weapons, {link_d}. Do you accept?</i>", 
            reply_markup=kb, parse_mode="HTML"
        )
        
        # Re-use your expiration logic
        asyncio.create_task(expire_pvp_challenge(brawl_games, m.from_user.id, msg))
    finally:
        active_games.discard(m.from_user.id)

def get_brawl_ui(game, c_name, d_name):
    """Generates the live Arena UI text and keyboard."""
    c_fighter = game['c_team'][game['c_idx']]
    d_fighter = game['d_team'][game['d_idx']]
    
    c_left = 3 - game['c_idx']
    d_left = 3 - game['d_idx']
    
    c_bar = make_hp_bar(c_fighter['hp'], c_fighter['max_hp'])
    d_bar = make_hp_bar(d_fighter['hp'], d_fighter['max_hp'])
    
    text = (
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⚔️ <b>𝗦𝗬𝗡𝗗𝗜𝗖𝗔𝗧𝗘 𝗕𝗥𝗔𝗪𝗟</b> ⚔️\n"
        f"💰 <b>Pot:</b> {game['bet'] * 2:,} Gold\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"🩸 <b>{c_name}</b> ({c_left} Fighters Left)\n"
        f"{EMOJIS.get(c_fighter['rarity'], '✨')} [{c_fighter['rarity']}] <b>{c_fighter['name']}</b>\n"
        f"HP: <code>{c_bar}</code> {c_fighter['hp']}/{c_fighter['max_hp']}\n\n"
        f"🆚\n\n"
        f"🎯 <b>{d_name}</b> ({d_left} Fighters Left)\n"
        f"{EMOJIS.get(d_fighter['rarity'], '✨')} [{d_fighter['rarity']}] <b>{d_fighter['name']}</b>\n"
        f"HP: <code>{d_bar}</code> {d_fighter['hp']}/{d_fighter['max_hp']}\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📜 <b>ROUND {game['round']} LOG:</b>\n"
    )
    
    for log_line in game['log'][-2:]:
        # Strip out any potential unescaped HTML from the names just to be safe
        safe_log = log_line.replace("<", "&lt;").replace(">", "&gt;") 
        # But we added bold tags in our combat log, so we have to revert those specific ones
        safe_log = safe_log.replace("&lt;b&gt;", "<b>").replace("&lt;/b&gt;", "</b>")
        text += f"<i>{safe_log}</i>\n"
        
    text += f"━━━━━━━━━━━━━━━━━━\n👉 <b>Make your move!</b>\n\n"
    
    # Status Indicators
    c_status = "✅ <i>Locked in</i>" if game['moves'].get(game['challenger']) else "<i>thinking...</i>"
    d_status = "✅ <i>Locked in</i>" if game['moves'].get(game['defender']) else "<i>thinking...</i>"
    text += f"🩸 {c_name}: {c_status}\n🎯 {d_name}: {d_status}"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="🗡️ Strike", callback_data=f"bm_{game['challenger']}_strike"),
        types.InlineKeyboardButton(text="💥 Heavy", callback_data=f"bm_{game['challenger']}_heavy"),
        types.InlineKeyboardButton(text="🛡️ Defend", callback_data=f"bm_{game['challenger']}_defend")
    ]])
    return text, kb

@dp.callback_query(F.data.startswith("brawl_accept_"))
async def handle_brawl_accept(call: types.CallbackQuery):
    challenger_id = int(call.data.split("_")[2])
    game = brawl_games.get(challenger_id)
    
    if not game: return await call.answer("❌ Challenge expired.", show_alert=True)
    if call.from_user.id != game['defender']: return await call.answer("❌ This challenge is not for you!", show_alert=True)

    if call.from_user.id in active_games: return await call.answer("⏳ Processing...")
    active_games.add(call.from_user.id)
    active_games.add(challenger_id)
    
    try:
        p_d = await get_player_fast(call.from_user)
        if game['bet'] > p_d['balance']:
            await update_player_balance(challenger_id, game['bet']) # Refund Challenger
            del brawl_games[challenger_id]
            return await call.message.edit_text("❌ Opponent ran out of money! Challenge cancelled.")

        await update_player_balance(call.from_user.id, -game['bet']) # Escrow Defender
        
        # Build the structured teams
        def build_team(ids):
            team = []
            for cid in ids:
                c = CHARACTER_CACHE[cid]
                stats = BRAWL_STATS[c['rarity']]
                team.append({
                    "name": c['name'], "rarity": c['rarity'],
                    "hp": stats['hp'], "max_hp": stats['hp'],
                    "dmg": stats['dmg'], "spd": stats['spd']
                })
            return team
            
        game.update({
            "c_team": build_team(game['c_team_ids']), "d_team": build_team(game['d_team_ids']),
            "c_idx": 0, "d_idx": 0,
            "moves": {}, "round": 1, "log": ["<i>The arena gates close. FIGHT!</i>"],
            "c_name": html.escape((await bot.get_chat(challenger_id)).first_name),
            "d_name": html.escape(call.from_user.first_name),
            "status": "active" # <--- ADD THIS FLAG TO BLOCK THE TIMER
        })
        
        text, kb = get_brawl_ui(game, game['c_name'], game['d_name'])
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        
    finally:
        active_games.discard(call.from_user.id)
        active_games.discard(challenger_id)

@dp.callback_query(F.data.startswith("brawl_decline_"))
async def handle_brawl_decline(call: types.CallbackQuery):
    challenger_id = int(call.data.split("_")[2])
    game = brawl_games.get(challenger_id)
    
    if not game: return await call.answer("❌ Challenge expired.", show_alert=True)
    if call.from_user.id not in [game['defender'], game['challenger']]: return await call.answer("❌ Not your game.", show_alert=True)

    await update_player_balance(challenger_id, game['bet'])
    del brawl_games[challenger_id]
    await call.message.edit_text("❌ Challenge declined or cancelled. Gold refunded.")

@dp.callback_query(F.data.startswith("bm_"))
async def handle_brawl_move(call: types.CallbackQuery):
    parts = call.data.split("_")
    game_id = int(parts[1])
    move = parts[2]
    
    game = brawl_games.get(game_id)
    if not game: return await call.answer("❌ Game ended or expired.", show_alert=True)
    
    uid = call.from_user.id
    if uid not in [game['challenger'], game['defender']]:
        return await call.answer("❌ Not your fight!", show_alert=True)
        
    if uid in game['moves']:
        return await call.answer("⏳ You already locked in your move! Waiting for opponent...", show_alert=True)
        
    game['moves'][uid] = move
    
    # Not ready yet, just update the status UI
    if len(game['moves']) < 2:
        text, kb = get_brawl_ui(game, game['c_name'], game['d_name'])
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        return
        
    # --- RESOLUTION PHASE (Both locked in) ---
    c_fighter = game['c_team'][game['c_idx']]
    d_fighter = game['d_team'][game['d_idx']]
    
    m_c = game['moves'][game['challenger']]
    m_d = game['moves'][game['defender']]
    
    c_def = (m_c == "defend")
    d_def = (m_d == "defend")
    
    round_logs = []
    
    # Determine Initiative
    if c_fighter['spd'] > d_fighter['spd']: first, second = "c", "d"
    elif d_fighter['spd'] > c_fighter['spd']: first, second = "d", "c"
    else: first, second = random.choice([("c", "d"), ("d", "c")]) # Coin Toss
        
    def execute_attack(attacker_type):
        """Processes one side of the attack sequence"""
        if attacker_type == "c":
            atk_f, def_f, move_name, is_defending = c_fighter, d_fighter, m_c, d_def
            owner_name = game['c_name']
        else:
            atk_f, def_f, move_name, is_defending = d_fighter, c_fighter, m_d, c_def
            owner_name = game['d_name']
            
        if move_name == "defend":
            round_logs.append(f"— {owner_name} braced for impact! (Defending)")
            return False # Nobody died
            
        dmg, hit = roll_damage(atk_f['dmg'], move_name, is_defending)
        move_display = "Heavy Blow" if move_name == "heavy" else "Strike"
        
        if not hit:
            round_logs.append(f"— {owner_name} used {move_display} and <b>MISSED!</b>")
        else:
            def_f['hp'] -= dmg
            block_text = " (Blocked!)" if is_defending else ""
            round_logs.append(f"— {owner_name}'s {atk_f['name']} used {move_display}! (Dealt {dmg}{block_text})")
            
        if def_f['hp'] <= 0:
            def_f['hp'] = 0
            round_logs.append(f"💀 <b>{def_f['name']} was defeated!</b>")
            return True # Target died
        return False
        
    # 1. First Attacker Strikes
    killed = execute_attack(first)
    
    # 2. Second Attacker Strikes (Only if they didn't die)
    if not killed:
        killed = execute_attack(second)
        
    # Check for dead fighters and advance roster
    game['log'] = round_logs
    game['round'] += 1
    game['moves'] = {} # Reset for next round
    
    c_dead = game['c_team'][game['c_idx']]['hp'] <= 0
    d_dead = game['d_team'][game['d_idx']]['hp'] <= 0
    
    if c_dead: game['c_idx'] += 1
    if d_dead: game['d_idx'] += 1
    
    # 3. Check Game Over Condition
    if game['c_idx'] >= 3 or game['d_idx'] >= 3:
        winner = None
        if game['d_idx'] >= 3 and game['c_idx'] < 3: winner, loser_name, winner_name = game['challenger'], game['d_name'], game['c_name']
        elif game['c_idx'] >= 3 and game['d_idx'] < 3: winner, loser_name, winner_name = game['defender'], game['c_name'], game['d_name']

        if final_winner:
            pot = int(game['bet'] * 2 * 0.95) 
            p_win = await get_player_fast(types.User(id=final_winner, is_bot=False, first_name=""))
            await update_player_balance(final_winner, pot, {"wins": p_win.get('wins', 0) + 1})

            end_text = (
                f"━━━━━━━━━━━━━━━━━━\n"
                f"⚔️ <b>𝗕𝗥𝗔𝗪𝗟 𝗖𝗢𝗡𝗖𝗟𝗨𝗗𝗘𝗗</b> ⚔️\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"💀 <b>{loser_name}'s</b> entire roster was wiped out.\n\n"
                f"👑 <b>WINNER: {winner_name}</b>\n"
                f"They walk away with the <b>{pot:,} Gold</b> pot!\n\n"
                f"📜 <b>FINAL LOG:</b>\n"
            )
            for log_line in game['log']: end_text += f"<i>{log_line}</i>\n"
            
            await call.message.edit_text(end_text, parse_mode="HTML")
            # --- FIX: BRAWL AUDIT LOG ---
            asyncio.create_task(send_log(f"⚔️ <b>BRAWL WIN</b>\n<a href='tg://user?id={final_winner}'>{winner_name}</a> wiped out {loser_name}'s team and took a <b>{pot:,} Gold</b> pot!"))

        else:
            # Absolute Miracle Draw (Both died to something weird)
            await update_player_balance(game['challenger'], game['bet'])
            await update_player_balance(game['defender'], game['bet'])
            await call.message.edit_text("🤝 <b>Mutual Destruction!</b> Both rosters wiped. Gold refunded.", parse_mode="HTML")
            
        del brawl_games[game_id]
        return
        
    # 4. If game continues, edit UI for next round
    if c_dead: game['log'].append(f"🩸 {game['c_name']} sends out <b>{game['c_team'][game['c_idx']]['name']}</b>!")
    if d_dead: game['log'].append(f"🎯 {game['d_name']} sends out <b>{game['d_team'][game['d_idx']]['name']}</b>!")
    
    text, kb = get_brawl_ui(game, game['c_name'], game['d_name'])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


# --- 13. GACHA PVP (DUEL & DRAFT BO3) ---
async def expire_pvp_challenge(game_dict, challenger_id, msg: types.Message):
    """Automatically cancels the challenge if not accepted in 60 seconds."""
    await asyncio.sleep(60)
    if challenger_id in game_dict:
        # --- THE FIX: Do not expire if the fight is actively happening! ---
        if game_dict[challenger_id].get('status') == 'active':
            return 
            
        game = game_dict.pop(challenger_id)
        await update_player_balance(challenger_id, game['bet']) # Refund
        try:
            await msg.edit_text("❌ <b>Challenge Expired!</b>\nThe opponent didn't answer in time. Gold refunded.", parse_mode="HTML", reply_markup=None)
        except Exception:
            pass

async def execute_gacha_battle_bo3(call: types.CallbackQuery, p_c: dict, p_d: dict, c_team: list, d_team: list, bet: int, mode: str):
    """Executes the Best of 3 Battle with live commentary delays."""
    c_score, d_score = 0, 0
    safe_c_name = html.escape(p_c.get('username') or p_c.get('first_name') or "Challenger")
    safe_d_name = html.escape(p_d.get('username') or p_d.get('first_name') or "Defender")
    
    link_c = f"<a href='tg://openmessage?user_id={p_c['id']}'>@{safe_c_name}</a>"
    link_d = f"<a href='tg://openmessage?user_id={p_d['id']}'>@{safe_d_name}</a>"

    main_msg = await call.message.edit_text(f"⚔️ <b>MATCH ACCEPTED!</b>\n<i>Drafting fighters from the void...</i>", parse_mode="HTML")
    await asyncio.sleep(2.5)
    
    mode_title = "𝗦𝗬𝗡𝗗𝗜𝗖𝗔𝗧𝗘 𝗦𝗛𝗢𝗪𝗗𝗢𝗪𝗡" if mode == "duel" else "𝗖𝗛𝗔𝗢𝗦 𝗥𝗢𝗨𝗟𝗘𝗧𝗧𝗘"
    history = f"⚔️ <b>{mode_title} (𝗕𝗢𝟯)</b> ⚔️\n💰 Pot: <b>{bet * 2:,} Gold</b>\n━━━━━━━━━━━━━━━━━━\n\n"

    for round_num in range(1, 4):
        c_char = CHARACTER_CACHE[c_team[round_num-1]]
        d_char = CHARACTER_CACHE[d_team[round_num-1]]
        
        c_range = RARITY_RANGES.get(c_char['rarity'], (10, 40))
        d_range = RARITY_RANGES.get(d_char['rarity'], (10, 40))

        c_total = random.randint(c_range[0], c_range[1])
        d_total = random.randint(d_range[0], d_range[1])
        
        while c_total == d_total:
            c_total = random.randint(c_range[0], c_range[1])
            d_total = random.randint(d_range[0], d_range[1])

        winner_name = link_c if c_total > d_total else link_d
        if c_total > d_total: c_score += 1
        else: d_score += 1

        round_title = "🥊 <b>𝗥𝗢𝗨𝗡𝗗 𝟯 (𝗧𝗜𝗘𝗕𝗥𝗘𝗔𝗞𝗘𝗥!)</b>" if round_num == 3 else f"🥊 <b>𝗥𝗢𝗨𝗡𝗗 {round_num}</b>"
        round_text = (
            f"{round_title}\n"
            f"🩸 <b>{link_c}:</b> {EMOJIS.get(c_char['rarity'], '✨')} [{c_char['rarity']}] {c_char['name']} (Roll: {c_total})\n"
            f"🎯 <b>{link_d}:</b> {EMOJIS.get(d_char['rarity'], '✨')} [{d_char['rarity']}] {d_char['name']} (Roll: {d_total})\n"
            f"🏆 <i>Round goes to {winner_name}!</i>\n\n"
        )

        history += round_text
        
        if c_score == 2 or d_score == 2: break 
            
        if round_num < 3:
            await main_msg.edit_text(history + f"⏳ <i>Preparing Round {round_num + 1}...</i>", parse_mode="HTML")
            await asyncio.sleep(3) 

    overall_winner_name = link_c if c_score > d_score else link_d
    overall_winner_p = p_c if c_score > d_score else p_d
    
    payout = await process_win(overall_winner_p, bet * 2, bet)
    history += f"💥 <b>MATCH POINT: {overall_winner_name} WINS ({max(c_score, d_score)}-{min(c_score, d_score)})!</b>\nThey take home <b>{payout:,} gold</b> (after taxes)."
    await main_msg.edit_text(history, parse_mode="HTML")

    # --- ADD THE AUDIT LOG HERE ---
    mode_name = "DUEL" if mode == "duel" else "DRAFT"
    asyncio.create_task(send_log(f"⚔️ <b>{mode_name} WIN</b>\n{overall_winner_name} won the Bo3 match and took a <b>{payout:,} Gold</b> pot!"))

# --- /DUEL COMMAND ---
@dp.message(Command("duel"))
async def cmd_duel(m: types.Message):
    if not m.reply_to_message: return await m.answer("❌ You must reply to the user you want to duel!")
    target = m.reply_to_message.from_user
    if target.id == m.from_user.id or target.is_bot: return await m.answer("❌ Invalid target.")

    args = m.text.split()
    amount_str = next((arg for arg in args[1:] if arg.isdigit()), None)
    if not amount_str: return await m.answer("❌ Usage: `/duel [amount]` (Must reply to a user)", parse_mode="Markdown")
        
    amount = int(amount_str)
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    if m.from_user.id in duel_games or m.from_user.id in draft_games: return await m.answer("❌ You already have a pending challenge!")
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")

    active_games.add(m.from_user.id)
    try:
        p_c = await get_player_fast(m.from_user)
        p_d = await get_player_fast(target)
        
        if amount > p_c['balance']: return await m.answer("❌ Insufficient balance.")
        if amount > p_d['balance']: return await m.answer("❌ Opponent does not have enough gold.")
        
        c_owned = list(set(p_c.get('owned_characters') or []))
        d_owned = list(set(p_d.get('owned_characters') or []))
        if len(c_owned) < 3: return await m.answer("❌ You need at least 3 unique characters to duel! Use /summon.")
        if len(d_owned) < 3: return await m.answer("❌ Your opponent needs at least 3 unique characters to duel! Tell them to /summon.")

        await update_player_balance(m.from_user.id, -amount) # Escrow
        duel_games[m.from_user.id] = {"challenger": m.from_user.id, "defender": target.id, "bet": amount}

        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="Cross Blades ⚔️", callback_data=f"pvp_accept_duel_{m.from_user.id}"),
            types.InlineKeyboardButton(text="Coward Out 🏳️", callback_data=f"pvp_decline_duel_{m.from_user.id}")
        ]])
        
        safe_c = html.escape(m.from_user.first_name)
        safe_d = html.escape(target.first_name)
        link_c = f"<a href='tg://openmessage?user_id={m.from_user.id}'>{safe_c}</a>"
        link_d = f"<a href='tg://openmessage?user_id={target.id}'>{safe_d}</a>"
        
        msg = await m.answer(
            f"⚔️ <b>𝗦𝗬𝗡𝗗𝗜𝗖𝗔𝗧𝗘 𝗦𝗛𝗢𝗪𝗗𝗢𝗪𝗡</b> ⚔️\n"
            f"<i>A clash of personal empires.</i>\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"🩸 <b>{link_c}</b> has challenged <b>{link_d}</b>!\n\n"
            f"💰 <b>Wager:</b> {amount:,} Gold\n"
            f"🏆 <b>Total Pot:</b> {amount * 2:,} Gold\n\n"
            f"<b>[ 𝗧𝗛𝗘 𝗥𝗨𝗟𝗘𝗦 ]</b>\n"
            f"• 3 Fighters drafted from your <i>own</i> collections.\n"
            f"• Best-of-3 rounds. Highest power roll wins.\n\n"
            f"<i>Draw your weapons, {link_d}. Do you accept?</i>\n"
            f"⏳ <b>60 seconds to accept...</b>", 
            reply_markup=kb, parse_mode="HTML"
        )

        asyncio.create_task(expire_pvp_challenge(duel_games, m.from_user.id, msg))
    finally:
        active_games.discard(m.from_user.id)

# --- /DRAFT COMMAND ---
@dp.message(Command("draft"))
async def cmd_draft(m: types.Message):
    if not m.reply_to_message: return await m.answer("❌ You must reply to the user you want to draft against!")
    target = m.reply_to_message.from_user
    if target.id == m.from_user.id or target.is_bot: return await m.answer("❌ Invalid target.")

    args = m.text.split()
    amount_str = next((arg for arg in args[1:] if arg.isdigit()), None)
    if not amount_str: return await m.answer("❌ Usage: `/draft [amount]` (Must reply to a user)", parse_mode="Markdown")
        
    amount = int(amount_str)
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    if m.from_user.id in duel_games or m.from_user.id in draft_games: return await m.answer("❌ You already have a pending challenge!")
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")

    active_games.add(m.from_user.id)
    try:
        p_c = await get_player_fast(m.from_user)
        p_d = await get_player_fast(target)
        
        if amount > p_c['balance']: return await m.answer("❌ Insufficient balance.")
        if amount > p_d['balance']: return await m.answer("❌ Opponent does not have enough gold.")
        
        combined_pool = (p_c.get('owned_characters') or []) + (p_d.get('owned_characters') or [])
        unique_pool = list(set(combined_pool))
        
        if len(unique_pool) < 6: return await m.answer(f"❌ Not enough characters! The combined player pool must have at least 6 unique characters (Currently {len(unique_pool)}).")

        await update_player_balance(m.from_user.id, -amount) # Escrow
        draft_games[m.from_user.id] = {"challenger": m.from_user.id, "defender": target.id, "bet": amount}

        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="Spin the Wheel 🌪️", callback_data=f"pvp_accept_draft_{m.from_user.id}"),
            types.InlineKeyboardButton(text="Decline ❌", callback_data=f"pvp_decline_draft_{m.from_user.id}")
        ]])
        
        safe_c = html.escape(m.from_user.first_name)
        safe_d = html.escape(target.first_name)
        link_c = f"<a href='tg://openmessage?user_id={m.from_user.id}'>{safe_c}</a>"
        link_d = f"<a href='tg://openmessage?user_id={target.id}'>{safe_d}</a>"
        
        msg = await m.answer(
            f"🌪️ <b>𝗧𝗛𝗘 𝗖𝗛𝗔𝗢𝗦 𝗥𝗢𝗨𝗟𝗘𝗧𝗧𝗘</b> 🌪️\n"
            f"<i>Both collections merged. No turning back.</i>\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"🎲 <b>Challenger:</b> <b>{link_c}</b>\n"
            f"🎯 <b>Victim:</b> <b>{link_d}</b>\n\n"
            f"💰 <b>Wager:</b> {amount:,} Gold\n"
            f"🎴 <b>Combined Pool:</b> {len(unique_pool)} Unique Fighters\n\n"
            f"<b>[ 𝗧𝗛𝗘 𝗥𝗨𝗟𝗘𝗦 ]</b>\n"
            f"• Your collections are merged into a single deck.\n"
            f"• 3 Fighters blindly dealt to each player.\n"
            f"• Pure RNG. Best-of-3 takes the pot.\n\n"
            f"<i>Are you feeling lucky, {link_d}?</i>\n"
            f"⏳ <b>60 seconds to accept...</b>", 
            reply_markup=kb, parse_mode="HTML"
        )

        asyncio.create_task(expire_pvp_challenge(draft_games, m.from_user.id, msg))
    finally:
        active_games.discard(m.from_user.id)

# --- PVP ACCEPT / DECLINE HANDLERS ---
@dp.callback_query(F.data.startswith("pvp_accept_"))
async def handle_pvp_accept(call: types.CallbackQuery):
    parts = call.data.split("_")
    mode = parts[2]
    challenger_id = int(parts[3])
    
    game_dict = duel_games if mode == "duel" else draft_games
    game = game_dict.get(challenger_id)
    
    if not game: return await call.answer("❌ Challenge expired or cancelled.", show_alert=True)
    if call.from_user.id != game['defender']: return await call.answer("❌ This challenge is not for you!", show_alert=True)

    if call.from_user.id in active_games: return await call.answer("⏳ Processing...")
    
    active_games.add(call.from_user.id)
    active_games.add(challenger_id)
    
    try:
        p_d = await get_player_fast(call.from_user)
        if game['bet'] > p_d['balance']:
            await update_player_balance(challenger_id, game['bet'])
            del game_dict[challenger_id]
            return await call.message.edit_text("❌ Opponent ran out of money! Challenge cancelled.")

        await update_player_balance(call.from_user.id, -game['bet'])
        del game_dict[challenger_id]
        
        p_c = await get_player_fast(types.User(id=challenger_id, is_bot=False, first_name=""))
        
        if mode == "duel":
            c_owned = list(set(p_c['owned_characters']))
            d_owned = list(set(p_d['owned_characters']))
            c_team, d_team = random.sample(c_owned, 3), random.sample(d_owned, 3)
        else: # Draft
            combined_pool = list(set((p_c.get('owned_characters') or []) + (p_d.get('owned_characters') or [])))
            drafted = random.sample(combined_pool, 6)
            c_team, d_team = drafted[0:3], drafted[3:6]

        await execute_gacha_battle_bo3(call, p_c, p_d, c_team, d_team, game['bet'], mode)
        
    finally:
        active_games.discard(call.from_user.id)
        active_games.discard(challenger_id)

@dp.callback_query(F.data.startswith("pvp_decline_"))
async def handle_pvp_decline(call: types.CallbackQuery):
    parts = call.data.split("_")
    mode = parts[2]
    challenger_id = int(parts[3])
    
    game_dict = duel_games if mode == "duel" else draft_games
    game = game_dict.get(challenger_id)
    
    if not game: return await call.answer("❌ Challenge expired.", show_alert=True)
    if call.from_user.id != game['defender'] and call.from_user.id != game['challenger']: return await call.answer("❌ Not your game.", show_alert=True)

    await update_player_balance(challenger_id, game['bet'])
    del game_dict[challenger_id]
    await call.message.edit_text("❌ Challenge declined or cancelled. Gold refunded.")

# --- 11. CORE COMMANDS (Spin/Daily/Bank/Rob) ---
@dp.message(Command("explore"))
async def cmd_explore(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        
        # 1. Prevent entering a new pub if they are already in one
        if p.get('in_pub'):
            return await m.answer(f"🛑 <b>You are already inside {p['pub_name']}!</b>\nPlay /{p['pub_game']} or use /exitpub to leave.", parse_mode="HTML")
        
        # 2. Check the 1-Hour Exhaustion Cooldown
        if p.get('last_explore_time'):
            last_time = datetime.fromisoformat(p['last_explore_time'].replace('Z', '+00:00'))
            now = datetime.now(IST)
            if last_time.tzinfo is None: last_time = last_time.replace(tzinfo=timezone.utc)
            
            time_passed = (now - last_time).total_seconds()
            if time_passed < 3600: # 1 hour = 3600 seconds
                wait_mins = int((3600 - time_passed) // 60)
                return await m.answer(f"⏳ <b>You are exhausted from exploring.</b>\nRest for another <b>{wait_mins} minutes</b> before hitting the streets again.", parse_mode="HTML")
        
        # 3. Randomize the Pub (Weighted: Slums are common, VIP is rare)
        games = list(PUB_MATRIX.keys())
        chosen_game = random.choice(games)
        
        # Weights: 40% Slum, 30% Dive, 15% Mid, 10% High, 5% VIP
        tier_weights = [40, 30, 15, 10, 5] 
        chosen_pub = random.choices(PUB_MATRIX[chosen_game], weights=tier_weights, k=1)[0]
        
        # 4. Lock them inside and hand them the chips
        updates = {
            "in_pub": True,
            "pub_name": chosen_pub['name'],
            "pub_game": chosen_game,
            "pub_balance": chosen_pub['money'],
            "pub_plays": 0
        }
        p.update(updates)
        queue_db_update("players", updates, "id", m.from_user.id)
        
        game_emojis = {"coin": "🪙", "dice": "🎲", "slots": "🎰", "mines": "💣", "cards": "🃏"}
        e = game_emojis.get(chosen_game, "🎲")
        
        text = (
            f"🚶‍♂️ <b>You wandered into the underground...</b>\n\n"
            f"🍻 <b>Location:</b> {chosen_pub['name']}\n"
            f"💰 <b>House Money:</b> {chosen_pub['money']:,} Pub Coins\n"
            f"{e} <b>Game Allowed:</b> <code>/{chosen_game}</code>\n\n"
            f"<b>[ 𝗧𝗛𝗘 𝗕𝗢𝗨𝗡𝗖𝗘𝗥'𝗦 𝗥𝗨𝗟𝗘𝗦 ]</b>\n"
            f"1. You play with our chips, not your vault.\n"
            f"2. You must play at least <b>{chosen_pub['min_plays']} games</b> before leaving.\n"
            f"3. We take a <b>{int(chosen_pub['tax']*100)}% Laundering Tax</b> when you cash out.\n\n"
            f"<i>Type /{chosen_game} to start playing! Type /exitpub when you want to leave.</i>"
        )
        await m.answer(text, parse_mode="HTML")
        
    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("contest"))
async def cmd_contest(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    try:
        # Fetch top 10 players with a score greater than 0
        res = await async_db(supabase.table("players").select("id, username, contest_score").gt("contest_score", 0).order("contest_score", desc=True).limit(10))
        
        p = await get_player_fast(m.from_user)
        my_score = p.get('contest_score', 0)
        
        text = "🏆 <b>𝗧𝗛𝗘 𝗠𝗬𝗧𝗛𝗜𝗖 𝗥𝗔𝗖𝗘</b> 🏆\n"
        text += "<i>1-Week Referral Showdown</i>\n"
        text += "━━━━━━━━━━━━━━━━━━\n\n"
        
        if not res.data:
            text += "<i>No verified recruits yet. The throne is empty!</i>\n"
        else:
            for i, row in enumerate(res.data):
                rank = i + 1
                raw_name = html.escape(row.get('username') or f"User_{row['id']}")
                score = row.get('contest_score', 0)
                medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"<b>{rank}.</b>"
                text += f"{medal} <a href='tg://user?id={row['id']}'>{raw_name}</a> — <b>{score}</b> Validated\n"
        
        text += f"\n━━━━━━━━━━━━━━━━━━\n"
        text += f"🎯 <b>Your Verified Score:</b> {my_score}\n"
        text += "<i>*Recruits only count toward your score once they play enough games to reach Level 2!</i>"
        
        await m.answer(text, parse_mode="HTML", disable_web_page_preview=True)
        
    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("resetcontest"))
async def cmd_resetcontest(m: types.Message):
    if m.from_user.id != 7708811819: return # Emperor Only
    
    status = await m.answer("⏳ <b>Wiping the contest board...</b>", parse_mode="HTML")
    try:
        # 1. Wipe DB
        await async_db(supabase.table("players").update({"contest_score": 0}).gt("id", 0))
        
        # 2. Wipe RAM
        for p in PLAYER_CACHE.values():
            p['contest_score'] = 0
            
        await status.edit_text("✅ <b>Contest Board Reset!</b> Everyone is back to 0 points. You are clear to announce the race.")
    except Exception as e:
        await status.edit_text(f"❌ Error: {e}")


@dp.message(Command("exitpub"))
async def cmd_exitpub(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        
        # 1. Are they even in a pub?
        if not p.get('in_pub'):
            return await m.answer("❌ You are not currently in a pub! Use /explore to find one.")
            
        pub_name = p['pub_name']
        pub_game = p['pub_game']
        plays = p.get('pub_plays', 0)
        balance = p.get('pub_balance', 0)
        
        # Fetch pub stats to calculate taxes and rules
        pub_info = next((item for item in PUB_MATRIX[pub_game] if item["name"] == pub_name), None)
        if not pub_info: pub_info = {"tax": 0.20, "min_plays": 0} # Safe fallback
            
        now_iso = datetime.now(IST).isoformat()
        updates = {
            "in_pub": False,
            "pub_name": None,
            "pub_game": None,
            "pub_balance": 0,
            "pub_plays": 0,
            "last_explore_time": now_iso # Starts the 1-hour cooldown
        }
        
        # 2. Check if they went broke FIRST (Bypasses min_plays)
        if balance <= 0:
            p.update(updates)
            queue_db_update("players", updates, "id", m.from_user.id)
            return await m.answer(f"🗑️ <b>The Bouncer grabs you by the collar!</b>\n'You lost all our chips! Get out!'\n\nYou were thrown out of {pub_name} completely broke. Rest up for an hour.", parse_mode="HTML")

        # 3. Check Bouncer's Minimum Play Rule (Only if trying to cash out profits)
        if plays < pub_info['min_plays']:
            return await m.answer(f"🛑 <b>The Bouncer blocks the door!</b>\n'You took our free chips. Play at least <b>{pub_info['min_plays'] - plays} more games</b> before you leave.'", parse_mode="HTML")
            
        # 4. Calculate Tax and Deposit to Main Vault
        tax_amount = int(balance * pub_info['tax'])
        take_home = balance - tax_amount

        await update_player_balance(m.from_user.id, take_home, updates)
        p.update(updates)
        
        text = (
            f"🚪 <b>CASHING OUT OF {pub_name.upper()}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Final Pub Balance:</b> {balance:,}\n"
            f"💸 <b>Laundering Tax ({int(pub_info['tax']*100)}%):</b> -{tax_amount:,}\n\n"
            f"✅ <b>Take Home Profit:</b> <b>{take_home:,} Gold</b>\n"
            f"<i>The gold has been secured in your main vault. Cooldown started (1 Hour).</i>"
        )
        await m.answer(text, parse_mode="HTML")
        
    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("redeem"))
async def cmd_redeem(m: types.Message):
    # 1. Lock to the Main Group
    if m.chat.id != MAIN_GROUP_ID:
        return await m.answer("❌ <b>Codes can only be redeemed inside the Official Main Group!</b>\nJoin the group to claim rewards.", parse_mode="HTML")
    
    args = m.text.split()
    if len(args) < 2: return await m.answer("❌ <b>Usage:</b>\n`/redeem [code]`", parse_mode="HTML")
        
    code = args[1].lower()
    
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    
    try:
        p = await get_player_fast(m.from_user)
        claimed = p.get('claimed_codes') or []
        
        # 2. Check if already claimed
        if code in claimed:
            return await m.answer("❌ You have already claimed this code!")
            
        # --- THE FIX: ENFORCE THE WHITELIST FOR DAILY DROPS ---
        if code.startswith("midnight-"):
            user_whitelisted_codes = verified_hackers.get(m.from_user.id, set())
            if code not in user_whitelisted_codes:
                return await m.answer("❌ <b>ACCESS DENIED.</b>\nYou cannot redeem this code until you hack the Syndicate Firewall in my DMs!", parse_mode="HTML")

        # 3. Fetch Code from Database
        res = await async_db(supabase.table("promo_codes").select("*").eq("code", code))
        if not res.data:
            return await m.answer("❌ Invalid or expired code.")
            
        promo = res.data[0]
        
        # 4. Check Use Limits
        if promo['max_uses'] != -1 and promo['uses'] >= promo['max_uses']:
            return await m.answer("❌ This code has reached its maximum number of redemptions!")
            
        # 5. Process Rewards
        total_gold_reward = promo['reward_gold']
        new_chars = []
        owned = p.get('owned_characters') or []
        
        # Give Character Rewards (with the 50k duplicate rule)
        if promo['reward_commons'] > 0:
            commons_pool = [c for c in CHARACTER_CACHE.values() if c['rarity'] == "Common"]
            if len(commons_pool) < promo['reward_commons']:
                return await m.answer("❌ The Gacha pool doesn't have enough Common characters yet!")
                
            awarded_chars = random.sample(commons_pool, promo['reward_commons'])
            for char in awarded_chars:
                if char['id'] in owned:
                    total_gold_reward += 50000 # Duplicate compensation
                else:
                    owned.append(char['id'])
                    new_chars.append(char['name'])

        claimed.append(code)
        
        # 6. Apply Database Updates
        updates = {"claimed_codes": claimed, "owned_characters": owned}
        if new_chars and not p.get('last_collect_time'):
            updates['last_collect_time'] = datetime.now(IST).isoformat()
            
        await update_player_balance(m.from_user.id, total_gold_reward, updates)
        p['claimed_codes'] = claimed
        p['owned_characters'] = owned
        
        # Increment the usage count globally and Check Auto-Delete
        new_uses = promo['uses'] + 1
        if promo['max_uses'] != -1 and new_uses >= promo['max_uses']:
            await async_db(supabase.table("promo_codes").delete().eq("code", code))
            try:
                await bot.send_message(MAIN_GROUP_ID, f"🏁 <b>𝗧𝗛𝗘 𝗗𝗥𝗢𝗣 𝗜𝗦 𝗘𝗠𝗣𝗧𝗬!</b>\nAll 5 claims for the drop have been secured.", parse_mode="HTML")
            except: pass
        else:
            await async_db(supabase.table("promo_codes").update({"uses": new_uses}).eq("code", code))

        # 7. Format the Output
        text = f"🎁 <b>CODE REDEEMED: {code.upper()}</b> 🎁\n\n"
        
        if promo['reward_gold'] > 0:
            text += f"💰 <b>Gold Reward:</b> +{promo['reward_gold']:,}\n"
            
        if new_chars:
            text += "🌸 <b>New Characters Unlocked:</b>\n"
            for name in new_chars:
                text += f"— 🤍 {name} [Common]\n"
                
        # Calculate if they got duplicate compensation
        duplicate_gold = total_gold_reward - promo['reward_gold']
        if duplicate_gold > 0:
            text += f"\n💸 <b>Duplicate Character Compensation:</b>\n— +{duplicate_gold:,} gold\n"
            
        await m.answer(text, parse_mode="HTML")

    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("codes"))
async def cmd_codes(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    
    try:
        # 1. Fetch player data to see what they've claimed
        p = await get_player_fast(m.from_user)
        claimed = [c.lower() for c in (p.get('claimed_codes') or [])]
        
        # 2. Fetch all promo codes from the database
        res = await async_db(supabase.table("promo_codes").select("*"))
        if not res.data:
            return await m.answer("📭 <b>No active promo codes right now!</b>\nCheck back later for more loot.", parse_mode="HTML")
        
        text = "🎟️ <b>ACTIVE PROMO CODES</b> 🎟️\n<i>Here is the current loot available in the Midnight Casino:</i>\n\n"
        
        for promo in res.data:
            code_name = promo['code'].upper()
            gold = promo['reward_gold']
            commons = promo['reward_commons']
            max_uses = promo['max_uses']
            uses = promo['uses']
            
            # 3. Format Rewards
            rewards = []
            if gold > 0: rewards.append(f"{gold:,} Gold")
            if commons > 0: rewards.append(f"{commons}x Commons")
            reward_str = " + ".join(rewards) if rewards else "Mystery Gift"
            
            # 4. Format Uses
            if max_uses == -1:
                uses_str = "∞ (Infinite)"
            elif uses >= max_uses:
                uses_str = "0 (Expired)"
            else:
                uses_str = f"{max_uses - uses} / {max_uses} remaining!"
                
            # 5. Format Player Status
            if promo['code'].lower() in claimed:
                status_str = "✅ Claimed"
            elif max_uses != -1 and uses >= max_uses:
                status_str = "❌ Missed it!"
            else:
                status_str = f"❌ Not Claimed (Type `/redeem {promo['code'].lower()}`)"
                
            text += (
                f"🎁 <b>Code:</b> <code>{code_name}</code>\n"
                f"💰 <b>Reward:</b> {reward_str}\n"
                f"📉 <b>Uses Left:</b> {uses_str}\n"
                f"👤 <b>Your Status:</b> {status_str}\n\n"
            )
            
        # 6. The Main Group Reminder
        text += "<i>⚠️ Remember: Codes can only be redeemed inside the Official Main Group @MidnightCasinoBotGroup!</i>"
        
        await m.answer(text, parse_mode="HTML")
        
    except Exception as e:
        logging.error(f"Error in /codes: {e}")
        await m.answer("❌ Could not fetch codes right now.", parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("bounty"))
async def cmd_bounty(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    
    try:
        global BOUNTY_TARGET_ID, BOUNTY_AMOUNT, LAST_BOUNTY_TARGET_ID, LAST_BOUNTY_AMOUNT, LAST_BOUNTY_CLAIMER_ID
        
        # 1. If a bounty is currently active
        if BOUNTY_TARGET_ID:
            res = await async_db(supabase.table("players").select("username").eq("id", BOUNTY_TARGET_ID))
            if res.data:
                target_name = html.escape(res.data[0].get('username') or f"User_{BOUNTY_TARGET_ID}")
                target_tag = f"<a href='tg://user?id={BOUNTY_TARGET_ID}'>@{target_name}</a>"
                
                return await m.answer(
                    f"🎯 <b>ACTIVE BOUNTY</b> 🎯\n"
                    f"━━━━━━━━━━━━━━━━━━\n\n"
                    f"🩸 <b>Target:</b> {target_tag}\n"
                    f"💰 <b>Reward:</b> {BOUNTY_AMOUNT:,} gold\n\n"
                    f"<i>The first person to successfully /rob them claims this bonus from the House!</i>",
                    parse_mode="HTML"
                )
            else:
                return await m.answer("🎯 <b>ACTIVE BOUNTY</b>\nThe target is currently in hiding...", parse_mode="HTML")
                
        # 2. If board is empty, check for history
        history_text = ""
        if LAST_BOUNTY_TARGET_ID and LAST_BOUNTY_CLAIMER_ID:
            # Fetch names for the history log
            res_t = await async_db(supabase.table("players").select("username").eq("id", LAST_BOUNTY_TARGET_ID))
            res_c = await async_db(supabase.table("players").select("username").eq("id", LAST_BOUNTY_CLAIMER_ID))
            
            t_name = html.escape(res_t.data[0].get('username') or f"User_{LAST_BOUNTY_TARGET_ID}") if res_t.data else "Unknown"
            c_name = html.escape(res_c.data[0].get('username') or f"User_{LAST_BOUNTY_CLAIMER_ID}") if res_c.data else "Unknown"
            
            history_text = (
                f"\n\n📜 <b>LAST BOUNTY LOG:</b>\n"
                f"🩸 <b>Target:</b> {t_name}\n"
                f"🥷 <b>Claimed By:</b> {c_name}\n"
                f"💰 <b>Payout:</b> {LAST_BOUNTY_AMOUNT:,} gold"
            )
            
        await m.answer(
            f"🎯 <b>BOUNTY BOARD: EMPTY</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"The streets are quiet. Wait for the Midnight reset for a new target."
            f"{history_text}", 
            parse_mode="HTML"
        )
            
    except Exception as e:
        await m.answer(f"❌ Error checking the bounty board: {e}")
    finally:
        active_games.discard(m.from_user.id)

async def process_ad_reward(m: types.Message, token: str):
    """Safely processes an ad ticket and enforces hard database cooldowns."""
    if token not in AD_TOKENS:
        return await m.answer("❌ Invalid or expired ad session. Please generate a new link.")
        
    session = AD_TOKENS.pop(token)
    if session['user_id'] != m.from_user.id:
        return await m.answer("❌ This ad session belongs to someone else.")
        
    action = session['action']
    p = await get_player_fast(m.from_user)
    today_str = str(datetime.now(IST).date())
    
    if action == "spin":
        if p.get('last_spin_date') == today_str and p.get('has_used_ad_spin'):
            return await m.answer("❌ You have already used your bonus spin for today!")
        if p.get('ad_spin_unlocked'):
            return await m.answer("❌ You already unlocked your bonus spin. Type /spin to use it!")
            
        p['ad_spin_unlocked'] = True
        queue_db_update("players", {"ad_spin_unlocked": True}, "id", m.from_user.id)
        return await m.answer("✅ <b>Bonus Spin Unlocked!</b>\nType /spin to use it!", parse_mode="HTML")
        
    elif action == "daily":
        claimed_at = p.get('daily_claimed_at', '') or ''
        if claimed_at.startswith(today_str):
            return await m.answer("❌ You have already claimed your daily reward!")
            
        now_iso = datetime.now(IST).isoformat()
        p['daily_claimed_at'] = now_iso
        await update_player_balance(m.from_user.id, 50000, {"daily_claimed_at": now_iso}) # <-- 50K BASE
        
        ad_url_double = generate_ad_link(m.from_user.id, "daily_double")
        double_kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="🔥 DOUBLE IT (Watch 2nd Ad)", web_app=types.WebAppInfo(url=ad_url_double))
        ]])
        return await m.answer("✅ <b>50,000 Coins Collected!</b>\nWant to turn it into <b>100,000</b>? Watch one more ad!", reply_markup=double_kb, parse_mode="HTML")
        
    elif action == "daily_double":
        claimed_at = p.get('daily_claimed_at', '') or ''
        if not claimed_at.startswith(today_str):
            return await m.answer("❌ You must claim your normal daily reward first!")
        if "_DOUBLED" in claimed_at:
            return await m.answer("❌ You already doubled your reward today!")
            
        new_claimed = claimed_at + "_DOUBLED"
        p['daily_claimed_at'] = new_claimed
        await update_player_balance(m.from_user.id, 50000, {"daily_claimed_at": new_claimed}) # <-- 50K DOUBLE
        return await m.answer("🔥 <b>DOUBLE CLAIMED!</b>\nYou got an extra 50,000 gold!", parse_mode="HTML")
        
    elif action == "bank":
        if p.get('last_bank_reset') != today_str:
            p['bank_ads_count'] = 0
            p['last_bank_reset'] = today_str
            
        count = p.get('bank_ads_count', 0)
        if count >= 5:
            return await m.answer("🏦 Your daily bank tiers are exhausted! Come back tomorrow.")
            
        reward = (count + 1) * 50000 # <-- 50K PER TIER MULTIPLIER
        p['bank_ads_count'] = count + 1
        await update_player_balance(m.from_user.id, reward, {"bank_ads_count": count + 1, "last_bank_reset": today_str})
        return await m.answer(f"✅ <b>Tier {count+1} Unlocked!</b>\n+{reward:,} coins added.", parse_mode="HTML")

@dp.message(F.web_app_data)
async def handle_web_app_data(m: types.Message):
    """Catches the token when the Mini App closes via Telegram.WebApp.sendData()."""
    token = m.web_app_data.data
    await process_ad_reward(m, token)

@dp.message(Command("earn"))
async def cmd_earn(m: types.Message):
    # 1. Check the 5-Minute Cooldown
    now = datetime.now().timestamp()
    last_used = EARN_COOLDOWNS.get(m.from_user.id, 0)
    if now - last_used < 300: # 300 seconds = 5 minutes
        wait_time = int(300 - (now - last_used))
        mins, secs = divmod(wait_time, 60)
        return await m.answer(f"⏳ <b>Cooldown Active!</b>\nThe Bouncer is finding new contracts. Wait <b>{mins}m {secs}s</b>.", parse_mode="HTML")

    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    
    try:
        # 2. Check the Daily Limit (5 Max)
        p = await get_player_fast(m.from_user)
        today_str = str(datetime.now(IST).date())
        count = p.get('tasks_completed', 0) if p.get('last_task_date') == today_str else 0
        
        if count >= 5:
            return await m.answer("🛑 <b>Daily Limit Reached!</b>\nYou have already completed your 5 daily contracts. Come back tomorrow.", parse_mode="HTML")

        await m.chat.do("typing")
        clean_block_id = ''.join(filter(str.isdigit, str(ADSGRAM_BLOCK_ID)))
        
        api_url = (
            f"https://api.adsgram.ai/advbot"
            f"?tgid={m.from_user.id}"
            f"&blockid={clean_block_id}"
            f"&language=en"
            f"&token={ADSGRAM_TOKEN}"
        )
        
        # 3. Fetch from Adsgram
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(api_url) as resp:
                if resp.status != 200:
                    # Give a small 1-minute penalty if the server is busy so they don't spam the API
                    EARN_COOLDOWNS[m.from_user.id] = now - 240 
                    return await m.answer(f"🛑 <b>Ad Server Busy ({resp.status})</b>\nNo contracts available right now.", parse_mode="HTML")
                
                # THE FIX: Force it to read the data even if Adsgram sent it as 'text/plain'
                ad_data = await resp.json(content_type=None)

        # 4. Success! Lock in the 5-minute cooldown
        EARN_COOLDOWNS[m.from_user.id] = now

        ad_text = ad_data.get('text_html', 'Complete this sponsor task for gold!')
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text=f"🚀 {ad_data.get('button_name', 'Go!')}", url=ad_data['click_url'])],
            [types.InlineKeyboardButton(text=f"🎁 {ad_data.get('button_reward_name', 'Claim reward!')}", url=ad_data['reward_url'])]
        ])

        caption = (
            f"📢 <b>𝗦𝗬𝗡𝗗𝗜𝗖𝗔𝗧𝗘 𝗦𝗣𝗢𝗡𝗦𝗢𝗥</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{ad_text}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<i>Complete the task, then click claim to receive a Mystery Payout!</i>\n"
            f"🎯 <b>Daily Progress:</b> {count}/5"
        )

        if ad_data.get('image_url'):
            await m.answer_photo(photo=ad_data['image_url'], caption=caption, reply_markup=kb, parse_mode="HTML")
        else:
            await m.answer(caption, reply_markup=kb, parse_mode="HTML")
            
    except asyncio.TimeoutError:
        await m.answer("❌ Connection to Ad-Server timed out. The Syndicate network is unstable.")
    except Exception as e:
        logging.error(f"Earn Error: {repr(e)}")
        if isinstance(e, KeyError):
            await m.answer("🛑 <b>No Contracts Available</b>\nAdsgram doesn't have an ad for your region right now. Try again later.", parse_mode="HTML")
        else:
            # Escape the error so Telegram doesn't confuse it for HTML tags
            safe_error = html.escape(repr(e))
            await m.answer(f"❌ <b>Diagnostic Error:</b>\n<code>{safe_error}</code>", parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("start"))
async def cmd_start(m: types.Message, command: CommandObject = None):
    referrer_id = None
    args = command.args if command else None

    # --- 🚨 SECURE AD TICKET INTERCEPTOR 🚨 ---
    if args and args in AD_TOKENS:
        await process_ad_reward(m, args)
        return # STOP execution here so it doesn't show the lobby menu again

    # Handle Deep Links (Buttons from groups that send them to DMs)
    if args:
        if args == "spin": return await cmd_spin(m)
        elif args == "daily": return await cmd_daily(m)
        elif args == "bank": return await cmd_bank(m)
        elif args == "limit": return await cmd_stats(m)

        elif args.startswith("ad_reward_"):
            try:
                verified_id = int(args.split("_")[-1])
            except:
                return await m.answer("❌ Invalid reward link.")

            if verified_id != m.from_user.id:
                return await m.answer("❌ This reward belongs to another user.")

            p = await get_player_fast(m.from_user)
            today_str = str(datetime.now(IST).date())
            
            # Check the 5 per day limit
            count = p.get('tasks_completed', 0) if p.get('last_task_date') == today_str else 0
            if count >= 5:
                return await m.answer("🛑 <b>Daily Limit Reached</b>\nYou've finished your 5 daily contracts. Come back tomorrow.", parse_mode="HTML")

            # 🎲 DYNAMIC REWARD: Random payout between 15k and 50k
            reward = random.randint(15000, 50000)
            
            await update_player_balance(m.from_user.id, reward, {
                "tasks_completed": count + 1, 
                "last_task_date": today_str
            })
            
            return await m.answer(
                f"✅ <b>𝗧𝗔𝗦𝗞 𝗩𝗘𝗥𝗜𝗙𝗜𝗘𝗗</b>\n"
                f"💰 <b>{reward:,} Gold</b> wired to your vault!\n"
                f"<i>(Daily Progress: {count + 1}/5)</i>",
                parse_mode="HTML"
            )

        elif args.startswith("drop_"):
            code = args.split("_")[1].upper()
            
            # 1. Check if the code is still valid in the DB
            res = await async_db(supabase.table("promo_codes").select("*").eq("code", code.lower()))
            if not res.data or res.data[0]['uses'] >= res.data[0]['max_uses']:
                return await m.answer("❌ <b>TOO LATE.</b>\nThis drop has already been fully claimed and deleted!", parse_mode="HTML")
                
            # 2. Setup the Unique Anagram Task & Clue
            CIPHER_WORDS = {
                "ROULETTE": "A spinning wheel of chance.",
                "JACKPOT": "The ultimate massive payout.",
                "CASINO": "The House always wins here.",
                "SMUGGLER": "Moves illegal goods in the shadows.",
                "VAULT": "Where the Syndicate hides its gold.",
                "BOUNTY": "A price placed on someone's head.",
                "HEIST": "A highly coordinated robbery.",
                "BLACKJACK": "Get to 21, but don't bust.",
                "ENFORCER": "The muscle of the Syndicate.",
                "CROUPIER": "The dealer at the table.",
                "SYNDICATE": "Our underground criminal family."
            }
            
            # Pick a random, unique word for this specific user
            word = random.choice(list(CIPHER_WORDS.keys()))
            clue = CIPHER_WORDS[word]
            
            scrambled = list(word)
            random.shuffle(scrambled)
            while "".join(scrambled) == word: random.shuffle(scrambled)
            scrambled_str = "".join(scrambled)
            
            active_drop_tasks[m.from_user.id] = {"word": word, "code": code}
            
            return await m.answer(
                f"🔒 <b>𝗙𝗜𝗥𝗘𝗪𝗔𝗟𝗟 𝗔𝗖𝗧𝗜𝗩𝗘</b>\n"
                f"<i>To extract the promo code, decrypt this personal cipher:</i>\n\n"
                f"🧩 <b>Cipher:</b> <code>{scrambled_str}</code>\n"
                f"💡 <b>Clue:</b> <i>{clue}</i>\n\n"
                f"👉 <b>Type your decoded answer below:</b>",
                parse_mode="HTML"
            )

        elif args.startswith("ref_"):
            try:
                temp_id = int(args.split("_")[1])
                if temp_id != m.from_user.id: 
                    referrer_id = temp_id 
            except: 
                pass

    player = await get_player_fast(m.from_user)
    if not player: return await m.answer("⚠️ Connection Error.")

    # Referral Logic
    if referrer_id and not player.get('referred_by') and player.get('wins', 0) == 0 and player.get('total_losses', 0) == 0:
        player['referred_by'] = referrer_id
        queue_db_update("players", {"referred_by": referrer_id}, "id", m.from_user.id)
        try:
            safe_name = html.escape(m.from_user.username or m.from_user.first_name)
            await bot.send_message(
                referrer_id, 
                f"👤 <b>New Referral!</b>\n@{safe_name} just joined using your link. You'll now earn 5% of their wins!",
                parse_mode="HTML"
            )
            # --- NEW: SEND AUDIT LOG ---
            asyncio.create_task(send_log(f"🤝 <b>NEW REFERRAL</b>\n<a href='tg://user?id={m.from_user.id}'>{safe_name}</a> joined using User ID: <code>{referrer_id}</code>'s link!"))
        except Exception as e:
            logging.warning(f"Could not notify referrer {referrer_id}: {e}")

    # Generate and send the new Interactive Lobby
    bot_me = await bot.get_me()
    is_private = m.chat.type == "private"
    text, kb = get_start_screen(m.from_user.id, "main", is_private, bot_me.username)
    
    await m.answer(text, reply_markup=kb, reply_to_message_id=m.message_id, parse_mode="HTML")

@dp.message(Command("bal"))
async def cmd_balance(m: types.Message):
    p = await get_player_fast(m.from_user)
    defense = p.get('active_defense', 'None')
    health = p.get('defense_health', 0)
    
    defense_text = ""
    if defense and defense != 'None':
        heart_emoji = "❤️" if health > 1 else "💔"
        defense_text = f"\n🛡️ <b>Defense:</b> {defense} ({health} {heart_emoji})"

    text = (
        f"💰 <b>Your Vault Balance</b>\n"
        f"————————————————\n"
        f"🟡 Gold: <b>{p['balance']:,}</b>"
        f"{defense_text}\n"
    )

    # --- NEW: SHOW PUB STATUS ---
    if p.get('in_pub'):
        pub_game = p.get('pub_game', 'unknown')
        game_emoji = {"coin": "🪙", "dice": "🎲", "slots": "🎰", "mines": "💣", "cards": "🃏"}.get(pub_game, "🍻")
        
        text += (
            f"\n━━━━━━━━━━━━━━━━━━\n"
            f"🍻 <b>CURRENTLY EXPLORING</b>\n"
            f"📍 <b>Location:</b> {p.get('pub_name')}\n"
            f"{game_emoji} <b>Pub Chips:</b> {p.get('pub_balance', 0):,}\n"
            f"🎮 <b>Required Game:</b> /{pub_game}\n"
            f"🔄 <b>Games Played:</b> {p.get('pub_plays', 0)}\n"
            f"<i>(Use /exitpub to cash out)</i>"
        )

    await m.answer(text, reply_to_message_id=m.message_id, parse_mode="HTML")

@dp.message(Command("daily"))
async def cmd_daily(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        today_str = str(datetime.now(IST).date())
        claimed_at = p.get('daily_claimed_at', '') or ''
        
        if claimed_at.startswith(today_str):
            return await m.answer("⏳ <b>The vault is locked!</b>\nCome back tomorrow.", reply_to_message_id=m.message_id, parse_mode="HTML")

        if m.chat.type == "private":
            kb = types.InlineKeyboardMarkup(inline_keyboard=[[
                 types.InlineKeyboardButton(text="📺 Claim 50,000 (Watch Ad)", web_app=types.WebAppInfo(url=generate_ad_link(m.from_user.id, "daily")))
            ]])
        else:
            bot_user = await bot.get_me()
            kb = types.InlineKeyboardMarkup(inline_keyboard=[[
                types.InlineKeyboardButton(text="🎁 CLAIM DAILY COINS", url=f"https://t.me/{bot_user.username}?start=daily")
            ]])
        await m.answer("📅 <b>Daily Reward Available!</b>", reply_markup=kb, reply_to_message_id=m.message_id, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("bank"))
async def cmd_bank(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        today_str = str(datetime.now(IST).date())
        
        # 1. Handle Ad Ladder Reset
        if p.get('last_bank_reset') != today_str:
            p['bank_ads_count'] = 0
            p['last_bank_reset'] = today_str
            queue_db_update("players", {"bank_ads_count": 0, "last_bank_reset": today_str}, "id", m.from_user.id)

        # 2. Fetch Balances & Interest
        count = p.get('bank_ads_count', 0)
        bank_bal = p.get('bank_balance', 0)
        vault_bal = p.get('balance', 0)
        interest = calculate_unclaimed_interest(p)

        # 3. Build UI
        text = (
            f"🏦 <b>THE MIDNIGHT BANK</b> 🏦\n"
            f"<i>Offshore storage. 2% Daily Yield.</i>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🟡 <b>Vault Balance:</b> {vault_bal:,} Gold\n"
            f"🔒 <b>Locked in Bank:</b> {bank_bal:,} Gold\n"
            f"📈 <b>Unclaimed Interest:</b> +{interest:,} Gold\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<i>Use /deposit [amt] and /withdraw [amt] to move gold!</i>"
        )

        kb_rows = []
        
        # Claim Interest Button
        if bank_bal > 0:
            kb_rows.append([types.InlineKeyboardButton(text=f"💰 Claim Interest (+{interest:,})", callback_data=f"bank_claim_{m.from_user.id}")])

        # Ad Ladder Button
        if count < 5:
            reward = (count + 1) * 50000 # <-- MATCH THE NEW 50K MULTIPLIER
            if m.chat.type == "private":
                kb_rows.append([types.InlineKeyboardButton(text=f"📺 Watch Ad for Bonus (Tier {count+1}/5 - {reward:,}g)", web_app=types.WebAppInfo(url=generate_ad_link(m.from_user.id, "bank")))])
            else:
                bot_user = await bot.get_me()
                kb_rows.append([types.InlineKeyboardButton(text="🏦 CLAIM AD BONUS", url=f"https://t.me/{bot_user.username}?start=bank")])
        else:
            kb_rows.append([types.InlineKeyboardButton(text="🏦 Daily Ad Tiers Exhausted", callback_data="noop")])

        await m.answer(text, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=kb_rows), reply_to_message_id=m.message_id, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

@dp.callback_query(F.data.startswith("bank_claim_"))
async def handle_bank_claim(call: types.CallbackQuery):
    owner_id = int(call.data.split("_")[2])
    if call.from_user.id != owner_id: return await call.answer("❌ Not your bank account!", show_alert=True)
    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    
    active_games.add(call.from_user.id)
    try:
        p = await get_player_fast(call.from_user)
        interest = calculate_unclaimed_interest(p)
        
        if interest <= 0:
            return await call.answer("❌ No interest generated yet! Check back later.", show_alert=True)
            
        now_str = datetime.now(IST).isoformat()
        p['last_bank_interaction'] = now_str
        
        # Add interest to VAULT balance, update DB, reset timer
        await update_player_balance(call.from_user.id, interest, {"last_bank_interaction": now_str})
        
        await call.message.edit_text(
            f"✅ <b>INTEREST CLAIMED!</b>\n\n"
            f"<b>+{interest:,} Gold</b> has been deposited into your main vault.\n"
            f"<i>The interest timer has been reset.</i>", 
            parse_mode="HTML"
        )
    finally:
        active_games.discard(call.from_user.id)

@dp.message(Command("deposit", "dep"))
async def cmd_deposit(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    
    args = m.text.split()
    if len(args) < 2: return await m.answer("❌ <b>Usage:</b> `/deposit [amount]` or `/deposit all`", parse_mode="HTML")
    
    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        vault_bal = p.get('balance', 0)
        
        if args[1].lower() == "all": amount = vault_bal
        elif args[1].isdigit(): amount = int(args[1])
        else: return await m.answer("❌ Invalid amount.")
            
        if amount <= 0: return await m.answer("❌ Amount must be greater than 0.")
        if amount > vault_bal: return await m.answer("❌ You don't have that much gold in your vault!")
        
        # 1. Claim any pending interest before moving money!
        pending_interest = calculate_unclaimed_interest(p)
        
        # 2. Update RAM and DB
        now_str = datetime.now(IST).isoformat()
        p['bank_balance'] = p.get('bank_balance', 0) + amount
        p['last_bank_interaction'] = now_str
        
        # We subtract the deposit from the vault, but ADD the pending interest
        net_vault_change = pending_interest - amount 
        
        await update_player_balance(m.from_user.id, net_vault_change, {
            "bank_balance": p['bank_balance'],
            "last_bank_interaction": now_str
        })
        
        interest_text = f"\n📈 <i>Auto-claimed {pending_interest:,} gold in pending interest.</i>" if pending_interest > 0 else ""
        
        await m.answer(
            f"📥 <b>DEPOSIT SUCCESSFUL</b>\n"
            f"Moved <b>{amount:,} Gold</b> to your Offshore Bank.{interest_text}\n"
            f"🔒 <b>New Bank Balance:</b> {p['bank_balance']:,}",
            parse_mode="HTML"
        )
    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("withdraw", "with"))
async def cmd_withdraw(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    
    args = m.text.split()
    if len(args) < 2: return await m.answer("❌ <b>Usage:</b> `/withdraw [amount]` or `/withdraw all`", parse_mode="HTML")
    
    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        bank_bal = p.get('bank_balance', 0)
        
        if args[1].lower() == "all": amount = bank_bal
        elif args[1].isdigit(): amount = int(args[1])
        else: return await m.answer("❌ Invalid amount.")
            
        if amount <= 0: return await m.answer("❌ Amount must be greater than 0.")
        if amount > bank_bal: return await m.answer("❌ You don't have that much gold in your bank!")
        
        # 1. Claim pending interest
        pending_interest = calculate_unclaimed_interest(p)
        
        # 2. Update RAM and DB
        now_str = datetime.now(IST).isoformat()
        p['bank_balance'] = bank_bal - amount
        p['last_bank_interaction'] = now_str
        
        # Add the withdrawal amount AND pending interest to their active vault
        net_vault_change = amount + pending_interest
        
        await update_player_balance(m.from_user.id, net_vault_change, {
            "bank_balance": p['bank_balance'],
            "last_bank_interaction": now_str
        })
        
        interest_text = f"\n📈 <i>Auto-claimed {pending_interest:,} gold in pending interest.</i>" if pending_interest > 0 else ""
        
        await m.answer(
            f"📤 <b>WITHDRAWAL SUCCESSFUL</b>\n"
            f"Moved <b>{amount:,} Gold</b> to your active Vault.{interest_text}\n"
            f"🟡 <b>New Vault Balance:</b> {p['balance']:,}",
            parse_mode="HTML"
        )
    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("stats"))
async def cmd_stats(m: types.Message):
    p = await get_player_fast(m.from_user)
    wins = p.get('wins', 0)
    losses = p.get('total_losses', 0)
    robs_won = p.get('robs_won', 0)
    robs_lost = p.get('robs_lost', 0)
    
    total = wins + losses
    wr = (wins / total * 100) if total > 0 else 0
    safe_name = html.escape(m.from_user.username or m.from_user.first_name)

    level = p.get('level', 1)
    xp = p.get('xp', 0)
    next_level_xp = level * 100 # Calculates exactly how much XP they need for the next level
    
    text = (
        f"📊 <b>STATS: @{safe_name}</b>\n"
        f"————————————————\n"
        f"🎖️ <b>Level:</b> {level}\n"
        f"✨ <b>XP Progress:</b> {xp} / {next_level_xp} XP\n\n"
        f"✅ Wins: <b>{wins}</b>\n"
        f"❌ Losses: <b>{losses}</b>\n"
        f"📈 Win Rate: <b>{wr:.1f}%</b>\n\n"
        f"🥷 <b>Robbery Career:</b>\n"
        f"— Successful: <b>{robs_won}</b>\n"
        f"— Caught: <b>{robs_lost}</b>"
    )

    await m.answer(text, parse_mode="HTML")

@dp.message(Command("rob"))
async def cmd_rob(m: types.Message):
    if not m.reply_to_message:
        return await m.answer("🥷 Reply to the user you want to rob!")

    victim = m.reply_to_message.from_user
    victim_id = victim.id
    
    if m.from_user.id == victim_id:
        return await m.answer("❌ You cannot rob yourself!")
        
    # Block robbing bots
    if victim.is_bot:
        return await m.answer("❌ You cannot rob the House (or any other bot)!")

    if m.from_user.id in active_games or victim_id in active_games:
        return await m.answer("⏳ Please wait, someone is busy processing a game!")
    active_games.add(m.from_user.id)
    active_games.add(victim_id)

    try:
        robber = await get_player_fast(m.from_user)
        victim = await get_player_fast(m.reply_to_message.from_user)
        
        # --- 1. MINIMUM BALANCE CHECK (10k) ---
        if victim.get('balance', 0) < 10000:
            return await m.answer("❌ <b>Target too poor!</b>\nThe Syndicate doesn't rob the homeless. They need at least 10,000 gold.", parse_mode="HTML")

        now = datetime.now(IST) 
        
        # --- 2. ROBBER 24-HOUR COOLDOWN ---
        if robber.get('last_rob_time'):
            last_rob = datetime.fromisoformat(robber['last_rob_time'].replace('Z', '+00:00'))
            if last_rob.tzinfo is None: last_rob = last_rob.replace(tzinfo=timezone.utc)
            
            if now < last_rob + timedelta(hours=24):
                wait_time = (last_rob + timedelta(hours=24)) - now
                hours = int(wait_time.total_seconds() // 3600)
                mins = int((wait_time.total_seconds() % 3600) // 60)
                return await m.answer(f"⏳ <b>Police are patrolling!</b>\nLie low for another <b>{hours}h {mins}m</b> before attempting another heist.", parse_mode="HTML", reply_to_message_id=m.message_id)

        # Lock in the robber's timestamp
        robber['last_rob_time'] = now.isoformat()
        queue_db_update("players", {"last_rob_time": now.isoformat()}, "id", m.from_user.id)

        # --- 3. ACTIVE DEFENSE CHECK (Guards/Dogs) ---
        defense = victim.get('active_defense', 'None')
        if defense and defense != 'None':
            fine = min(5000, robber['balance']) 
            
            robber['robs_lost'] = robber.get('robs_lost', 0) + 1
            await update_player_balance(m.from_user.id, -fine, {"robs_lost": robber['robs_lost']})
            
            health = victim.get('defense_health', 0) - 1
            if health <= 0:
                victim['active_defense'] = "None"
                victim['defense_health'] = 0
                queue_db_update("players", {"active_defense": "None", "defense_health": 0}, "id", victim['id'])
                try:
                    safe_robber = html.escape(m.from_user.username or m.from_user.first_name)
                    await bot.send_message(victim['id'], f"⚠️ Your <b>{defense}</b> was used up protecting you from @{safe_robber}!", parse_mode="HTML")
                except Exception: pass
            else:
                victim['defense_health'] = health
                queue_db_update("players", {"defense_health": health}, "id", victim['id'])
        
            safe_victim = html.escape(m.reply_to_message.from_user.username or m.reply_to_message.from_user.first_name)
            safe_robber = html.escape(m.from_user.username or m.from_user.first_name)
            
            asyncio.create_task(send_log(f"🚨 <b>ROBBERY FAILED</b>\n<a href='tg://user?id={m.from_user.id}'>{safe_robber}</a> tried to rob <a href='tg://user?id={victim['id']}'>{safe_victim}</a> but was caught by a {defense}! (Fined: {fine:,})"))

            return await m.answer(
                f"🚓 <b>POLICE CAUGHT YOU!</b>\n\n"
                f"@{safe_victim} was protected by a <b>{defense}</b>.\n"
                f"You were fined <b>{fine:,} gold</b>.", parse_mode="HTML"
            )

        # --- 4. HEIST RNG (50% Chance) ---
        if random.random() > 0.5:
            # Check the Victim's 24-Hour Shield
            steal_percentage = 0.05 # Default 5%
            shield_active = False
            
            if victim.get('last_robbed_time'):
                last_robbed = datetime.fromisoformat(victim['last_robbed_time'].replace('Z', '+00:00'))
                if last_robbed.tzinfo is None: last_robbed = last_robbed.replace(tzinfo=timezone.utc)
                
                # If they were robbed less than 24 hours ago, apply the 0.5% shield
                if now < last_robbed + timedelta(hours=24):
                    steal_percentage = 0.005 # 0.5%
                    shield_active = True

            steal_amount = int(victim['balance'] * steal_percentage)
            if steal_amount <= 0: steal_amount = 1 # Minimum 1 gold

            robber['robs_won'] = robber.get('robs_won', 0) + 1

            # --- BOUNTY CLAIM LOGIC ---
            global BOUNTY_TARGET_ID, BOUNTY_AMOUNT, LAST_BOUNTY_TARGET_ID, LAST_BOUNTY_AMOUNT, LAST_BOUNTY_CLAIMER_ID
            bounty_bonus_text = ""
            log_bounty_tag = ""
            
            if victim['id'] == BOUNTY_TARGET_ID:
                await update_player_balance(m.from_user.id, BOUNTY_AMOUNT)
                bounty_bonus_text = f"\n\n💰 <b>BOUNTY CLAIMED!</b> The House paid you an extra <b>{BOUNTY_AMOUNT:,} gold</b> for the hit!"
                log_bounty_tag = f"\n🎯 <b>BOUNTY CLAIMED: +{BOUNTY_AMOUNT:,} Gold!</b>"
                
                LAST_BOUNTY_TARGET_ID = BOUNTY_TARGET_ID
                LAST_BOUNTY_AMOUNT = BOUNTY_AMOUNT
                LAST_BOUNTY_CLAIMER_ID = m.from_user.id
                
                BOUNTY_TARGET_ID = None 
                BOUNTY_AMOUNT = 0
            
            # Update balances and victim's last_robbed_time
            await update_player_balance(victim['id'], -steal_amount, {"last_robbed_time": now.isoformat()})
            await update_player_balance(m.from_user.id, steal_amount, {"robs_won": robber['robs_won']})
            
            safe_victim = html.escape(m.reply_to_message.from_user.username or m.reply_to_message.from_user.first_name)
            safe_robber = html.escape(m.from_user.username or m.from_user.first_name)
            
            asyncio.create_task(send_log(f"🥷 <b>ROBBERY SUCCESS</b>\n<a href='tg://user?id={m.from_user.id}'>{safe_robber}</a> stole <b>{steal_amount:,} Gold</b> from <a href='tg://user?id={victim['id']}'>{safe_victim}</a>!{log_bounty_tag}"))

            # Build the output message
            shield_msg = f"\n🛡️ <i>They had recently been robbed, so you only got away with 0.5% of their stash!</i>" if shield_active else ""
            await m.answer(f"🥷 <b>Success!</b> You stole <b>{steal_amount:,} gold</b> from {safe_victim}!{bounty_bonus_text}{shield_msg}", parse_mode="HTML")

        else:
            robber['robs_lost'] = robber.get('robs_lost', 0) + 1
            queue_db_update("players", {"robs_lost": robber['robs_lost']}, "id", m.from_user.id)
            await m.answer("🤫 You tripped over a trash can and alerted the neighborhood! The robbery failed.")

    finally:
        active_games.discard(m.from_user.id)
        active_games.discard(victim_id)

@dp.message(Command("pay", "transfer"))
async def cmd_pay(m: types.Message):
    # 1. Determine the target (Via Reply or User ID)
    target_user = None
    args = m.text.split()
    
    if m.reply_to_message:
        target_user = m.reply_to_message.from_user
        if len(args) < 2 or not args[1].isdigit():
            return await m.answer("❌ <b>Usage:</b> Reply to a user with <code>/pay [amount]</code>", parse_mode="HTML")
        amount = int(args[1])
    else:
        if len(args) < 3 or not args[1].isdigit() or not args[2].isdigit():
            return await m.answer("❌ <b>Usage:</b> <code>/pay [user_id] [amount]</code> OR reply to a user with <code>/pay [amount]</code>", parse_mode="HTML")
        target_id = int(args[1])
        amount = int(args[2])
        target_user = types.User(id=target_id, is_bot=False, first_name="Unknown")
        
    # 2. Basic Validation
    if target_user.id == m.from_user.id:
        return await m.answer("❌ You cannot send gold to yourself.")
    if target_user.is_bot:
        return await m.answer("❌ You cannot send gold to a bot.")
    if amount < 100:
        return await m.answer("❌ The Syndicate doesn't process pocket change. Minimum transfer is <b>100 gold</b>.", parse_mode="HTML")
        
    # 3. Lock both users to prevent race condition glitches
    if m.from_user.id in active_games or target_user.id in active_games:
        return await m.answer("⏳ Please wait, a transaction or game is currently processing for one of the users!")
        
    active_games.add(m.from_user.id)
    active_games.add(target_user.id)
    
    try:
        sender = await get_player_fast(m.from_user)
        if sender['balance'] < amount:
            return await m.answer("❌ Insufficient vault balance to cover the transfer.")
            
        recipient = await get_player_fast(target_user)
        if not recipient:
            return await m.answer("❌ Recipient not found in the database.")
            
        # 4. Calculate the 25% Money Laundering Tax
        tax = int(amount * 0.25)
        received = amount - tax
        
        # 5. Process the transaction
        await update_player_balance(m.from_user.id, -amount)
        await update_player_balance(target_user.id, received)
        
        # Format names securely
        s_name = html.escape(m.from_user.username or m.from_user.first_name)
        r_name = html.escape(target_user.username or target_user.first_name)
        if target_user.first_name == "Unknown":
            r_name = html.escape(recipient.get('username') or f"User_{target_user.id}")
            
        # 6. Fire the Overwatch Log
        asyncio.create_task(send_log(
            f"💸 <b>MONEY LAUNDERING</b>\n"
            f"<a href='tg://user?id={m.from_user.id}'>{s_name}</a> transferred <b>{amount:,} Gold</b> to <a href='tg://user?id={target_user.id}'>{r_name}</a>.\n"
            f"🏦 <b>House Tax (25%):</b> {tax:,}\n"
            f"✅ <b>Received:</b> {received:,}"
        ))
        
        # 7. Announce to the chat
        await m.answer(
            f"💸 <b>TRANSFER COMPLETE</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📤 <b>Sent by @{s_name}:</b> {amount:,} Gold\n"
            f"🏦 <b>House Tax (25%):</b> -{tax:,} Gold\n"
            f"📥 <b>@{r_name} Received:</b> {received:,} Gold",
            parse_mode="HTML"
        )
        
        # 8. Attempt to DM the recipient a receipt
        try:
            await bot.send_message(
                target_user.id, 
                f"💸 <b>FUNDS RECEIVED!</b>\n@{s_name} just wired you gold!\n\n"
                f"<b>Gross Amount:</b> {amount:,}\n"
                f"<b>Laundering Tax:</b> -{tax:,}\n"
                f"<b>Net Profit:</b> {received:,} Gold",
                parse_mode="HTML"
            )
        except Exception:
            pass # Fails silently if they blocked the bot
            
    finally:
        active_games.discard(m.from_user.id)
        active_games.discard(target_user.id)


@dp.message(Command("buy"))
async def cmd_buy(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        args = m.text.split()
        
        if len(args) < 2:
            return await m.answer(
                "🛒 <b>MIDNIGHT BLACK MARKET</b>\n\n"
                "1. <code>/buy dog</code> — Guard Dog (50k)\n"
                "   <i>Protects from 1 rob. Robber fined.</i>\n\n"
                "2. <code>/buy guard</code> — Bodyguard (100k)\n"
                "   <i>Protects from 2 robs. Robber fined.</i>",
               reply_to_message_id=m.message_id, parse_mode="HTML"
            )

        item = args[1].lower()
        if p.get('active_defense') and p.get('active_defense') != 'None':
            return await m.answer("❌ You already have active protection!", reply_to_message_id=m.message_id)

        if item == "dog":
            price, name, health = 50000, "Guard Dog", 1
        elif item == "guard":
            price, name, health = 100000, "Bodyguard", 2
        else:
            return await m.answer("❌ Item not found.", reply_to_message_id=m.message_id)

        if p['balance'] < price:
            return await m.answer("❌ You don't have enough gold!", reply_to_message_id=m.message_id)

        await update_player_balance(m.from_user.id, -price, {"active_defense": name, "defense_health": health})
        await m.answer(f"✅ Purchased <b>{name}</b>! You are now protected.", reply_to_message_id=m.message_id, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

# --- 12. STANDARD GAMES & PUB ROUTER ---

async def handle_bet(m: types.Message, p: dict, game_name: str, amount: int):
    """Intercepts the bet and routes it to either the Pub Wallet or the Main Vault."""
    if p.get('in_pub'):
        if p.get('pub_game') != game_name:
            await m.answer(f"🛑 <b>The bartender glares at you.</b>\n'Put that away. This is a {p.get('pub_game', '').capitalize()} joint. Play /{p.get('pub_game', '')} or get out.'", parse_mode="HTML")
            return False
        if amount > p.get('pub_balance', 0):
            await m.answer(f"❌ You only have <b>{p.get('pub_balance', 0):,} Pub Coins</b> left! Lower your bet or use /exitpub.", parse_mode="HTML")
            return False
        
        p['pub_balance'] -= amount
        p['pub_plays'] = p.get('pub_plays', 0) + 1
        queue_db_update("players", {"pub_balance": p['pub_balance'], "pub_plays": p['pub_plays']}, "id", p['id'])
        return "pub"
    else:
        if amount > p.get('balance', 0):
            await m.answer("❌ Insufficient vault balance.")
            return False
            
        await update_player_balance(p['id'], -amount)
        return "main"

async def handle_payout(p: dict, win_amount: int, bet_amount: int):
    """Deposits the winnings into the correct wallet and logs it."""
    safe_name = html.escape(p.get('username') or f"User_{p['id']}")
    
    if p.get('in_pub'):
        p['pub_balance'] = p.get('pub_balance', 0) + win_amount
        queue_db_update("players", {"pub_balance": p['pub_balance']}, "id", p['id'])
        
        # --- NEW: SEND AUDIT LOG ---
        asyncio.create_task(send_log(f"🍻 <b>PUB WIN</b>\n<a href='tg://user?id={p['id']}'>{safe_name}</a> won <b>{win_amount:,} Chips</b>! (Bet: {bet_amount:,})"))
        
        return win_amount, "Pub Coins"
    else:
        payout = await process_win(p, win_amount, bet_amount)
        
        # --- NEW: SEND AUDIT LOG ---
        asyncio.create_task(send_log(f"🎰 <b>CASINO WIN</b>\n<a href='tg://user?id={p['id']}'>{safe_name}</a> won <b>{payout:,} Gold</b>! (Bet: {bet_amount:,})"))
        
        return payout, "gold"

async def handle_loss(p: dict):
    """Only logs losses for the main game to protect their real Win/Loss Ratio."""
    if not p.get('in_pub'):
        p['total_losses'] = p.get('total_losses', 0) + 1
        queue_db_update("players", {"total_losses": p['total_losses']}, "id", p['id'])

@dp.message(Command("coin"))
async def cmd_coin(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait for your current game to finish!")
    if m.from_user.id in coin_games: return await m.answer("❌ You have an active coin flip! Play it first.")

    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit(): return await m.answer("❌ Usage: /coin [amount]")
    
    amount = int(args[1])
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        wallet = await handle_bet(m, p, "coin", amount)
        if not wallet: return

        coin_games[m.from_user.id] = {"bet": amount, "ts": datetime.now().timestamp()}

        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="🪙 Heads", callback_data=f"coin_heads_{m.from_user.id}"),
            types.InlineKeyboardButton(text="🪙 Tails", callback_data=f"coin_tails_{m.from_user.id}")
        ]])
        await m.answer(f"<b>🪙 Coin Flip</b>\nBet: {amount} coins\nPick your side:", reply_markup=kb, reply_to_message_id=m.message_id, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

@dp.callback_query(F.data.startswith("coin_"))
async def handle_coin_flip(call: types.CallbackQuery):
    parts = call.data.split("_")
    choice = parts[1]
    user_id = int(parts[2])

    if call.from_user.id != user_id:
        return await call.answer("❌ This is not your game!", show_alert=True)
        
    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)

    try:
        game = coin_games.pop(call.from_user.id, None)
        if not game: return await call.answer("❌ Game expired or already played.", show_alert=True)
        amount = game['bet']

        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

        p = await get_player_fast(call.from_user)

        result = random.choice(["heads", "tails"])
        await call.message.edit_text(f"✨ The coin is in the air...")
        await asyncio.sleep(1.5)

        if choice == result:
            amt, currency = await handle_payout(p, amount * 2, amount)
            await call.message.edit_text(f"🎉 <b>It's {result.upper()}!</b>\nYou won {amt:,} {currency}!", parse_mode="HTML")
        else:
            await handle_loss(p)
            currency = "Pub Coins" if p.get('in_pub') else "gold"
            await call.message.edit_text(f"💀 <b>It's {result.upper()}...</b>\nYou lost {amount:,} {currency}.", parse_mode="HTML")
    finally:
        active_games.discard(call.from_user.id)

@dp.message(Command("crates"))
async def cmd_crates(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait for your current game to finish!")
    if m.from_user.id in crate_games: return await m.answer("❌ You already have an active smuggler's drop! Finish it first.")

    args = m.text.split()
    # Support both /crates [crates] [amount] AND /crates [amount] [crates]
    if len(args) < 3 or not args[1].isdigit() or not args[2].isdigit(): 
        return await m.answer("❌ <b>Usage:</b> <code>/crates [3-9 crates] [amount]</code>", parse_mode="HTML")

    val1, val2 = int(args[1]), int(args[2])
    
    # Figure out which argument is the crates and which is the bet
    if 3 <= val1 <= 9: c_count, amount = val1, val2
    elif 3 <= val2 <= 9: c_count, amount = val2, val1
    else: return await m.answer("❌ You must spawn between <b>3 and 9 crates</b>.", parse_mode="HTML")

    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        wallet = await handle_bet(m, p, "crates", amount)
        if not wallet: return

        # Hide the smuggled gold in exactly one crate (0-indexed)
        winning_idx = random.randint(0, c_count - 1)
        
        crate_games[m.from_user.id] = {
            "bet": amount, 
            "c_count": c_count, 
            "winner": winning_idx,
            "ts": datetime.now().timestamp()
        }

        await m.answer(
            f"📦 <b>𝗧𝗛𝗘 𝗦𝗠𝗨𝗚𝗚𝗟𝗘𝗥'𝗦 𝗗𝗥𝗢𝗣</b> 📦\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Wager:</b> {amount:,} Gold\n"
            f"✨ <b>Jackpot Multiplier:</b> {CRATE_MULTIPLIERS[c_count]}x\n\n"
            f"<i>Only one crate holds the gold. Pick wisely...</i>", 
            reply_markup=get_crates_kb(m.from_user.id, c_count), 
            reply_to_message_id=m.message_id, 
            parse_mode="HTML"
        )
    finally:
        active_games.discard(m.from_user.id)

def get_crates_kb(user_id, count, revealed_idx=None, winner_idx=None):
    """Generates the row of Crates dynamically."""
    buttons = []
    for i in range(count):
        # Reveal State
        if revealed_idx is not None:
            if i == winner_idx: btn_text = "💰"
            else: btn_text = "🪵"
        # Hidden State
        else:
            btn_text = f"📦 {i+1}"
            
        buttons.append(types.InlineKeyboardButton(text=btn_text, callback_data=f"crate_{user_id}_{i}"))
    
    # Wrap buttons to max 5 per row so Telegram doesn't squish them
    kb = [buttons[i:i + 5] for i in range(0, len(buttons), 5)]
    return types.InlineKeyboardMarkup(inline_keyboard=kb)

@dp.callback_query(F.data.startswith("crate_"))
async def handle_crate_click(call: types.CallbackQuery):
    parts = call.data.split("_")
    user_id = int(parts[1])
    clicked_idx = int(parts[2])

    if call.from_user.id != user_id:
        return await call.answer("❌ These aren't your crates!", show_alert=True)
        
    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        game = crate_games.pop(call.from_user.id, None)
        if not game: return await call.answer("❌ Game expired or already opened.", show_alert=True)

        amount = game['bet']
        c_count = game['c_count']
        winner_idx = game['winner']
        mult = CRATE_MULTIPLIERS[c_count]
        
        p = await get_player_fast(call.from_user)

        # Build the revealed UI
        kb = get_crates_kb(user_id, c_count, revealed_idx=clicked_idx, winner_idx=winner_idx)

        if clicked_idx == winner_idx:
            # THE JACKPOT
            win_amount = int(amount * mult)
            amt, currency = await handle_payout(p, win_amount, amount)
            
            await call.message.edit_text(
                f"🎉 <b>𝗝𝗔𝗖𝗞𝗣𝗢𝗧!</b> 🎉\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"You picked the right crate!\n"
                f"<b>({mult}x Payout!)</b> You won <b>{amt:,} {currency}</b>!", 
                reply_markup=kb, parse_mode="HTML"
            )
        else:
            # THE SCRAP REFUND (0% to 50%)
            await handle_loss(p)
            scrap_percent = random.uniform(0.0, 0.50)
            salvaged = int(amount * scrap_percent)
            lost = amount - salvaged
            
            currency = "Pub Coins" if p.get('in_pub') else "gold"
            
            # Refund the scraps
            if salvaged > 0:
                if p.get('in_pub'):
                    p['pub_balance'] = p.get('pub_balance', 0) + salvaged
                    queue_db_update("players", {"pub_balance": p['pub_balance']}, "id", p['id'])
                else:
                    await update_player_balance(p['id'], salvaged)
                    
            percent_display = int(scrap_percent * 100)
            
            if salvaged == 0:
                refund_text = "💀 <b>Empty!</b> You opened a decoy crate.\nYou salvaged <b>0%</b> of your stash. The House takes it all."
            else:
                refund_text = f"📦 <b>Empty!</b> You opened a decoy crate.\nYou managed to salvage <b>{percent_display}%</b> of your stash! (+{salvaged:,} {currency})"
                
            await call.message.edit_text(
                f"{refund_text}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"💸 Net Loss: -{lost:,} {currency}", 
                reply_markup=kb, parse_mode="HTML"
            )
            
    finally:
        active_games.discard(call.from_user.id)

@dp.message(Command("slots"))
async def cmd_slots(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait for your current spin to finish!")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit(): return await m.answer("❌ Usage: /slots [amount]")
    
    amount = int(args[1])
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        wallet = await handle_bet(m, p, "slots", amount)
        if not wallet: return

        msg = await m.answer_dice(emoji="🎰")
        val = msg.dice.value
        await asyncio.sleep(3.5) 

        if val == 64: multiplier, result_text = 10, "🎰 <b>JACKPOT (777)!</b>"
        elif val in [1, 22, 43]: multiplier, result_text = 5, "✨ <b>TRIPLE!</b>"
        elif val in [2, 3, 4, 5, 6, 9, 11, 13, 16, 17, 18, 21, 23, 24, 26, 27, 30, 32, 33, 35, 38, 39, 41, 42, 44, 47, 48, 49, 52, 54, 56, 59, 60, 61, 62, 63]:
            multiplier, result_text = 1.5, "🃏 <b>DOUBLE!</b>"
        else:
            multiplier, result_text = 0, "❌ <b>No luck!</b>"

        if multiplier > 0:
            amt, currency = await handle_payout(p, int(amount * multiplier), amount)
            await m.answer(f"{result_text}\nYou won {amt:,} {currency}!", parse_mode="HTML")
        else:
            await handle_loss(p)
            await m.answer(f"{result_text} Try again.", parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("dart"))
async def cmd_dart(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait for your current game to finish!")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit(): return await m.answer("❌ Usage: /dart [amount]")
    
    amount = int(args[1])
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        wallet = await handle_bet(m, p, "dart", amount)
        if not wallet: return

        msg = await m.answer_dice(emoji="🎯")
        val = msg.dice.value
        await asyncio.sleep(2.5) # Dart animation is fast

        if val == 6:
            multiplier, result_text = 3.0, "🎯 <b>BULLSEYE! (3x)</b>"
        elif val == 5:
            multiplier, result_text = 1.5, "🔵 <b>Inner Ring! (1.5x)</b>"
        else:
            multiplier, result_text = 0, "❌ <b>Missed the mark!</b>"

        if multiplier > 0:
            amt, currency = await handle_payout(p, int(amount * multiplier), amount)
            await m.answer(f"{result_text}\nYou won {amt:,} {currency}!", parse_mode="HTML")
        else:
            await handle_loss(p)
            currency = "Pub Coins" if p.get('in_pub') else "gold"
            await m.answer(f"{result_text}\nYou lost {amount:,} {currency}.", parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)


@dp.message(Command("bowling"))
async def cmd_bowling(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait for your current game to finish!")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit(): return await m.answer("❌ Usage: /bowling [amount]")
    
    amount = int(args[1])
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        wallet = await handle_bet(m, p, "bowling", amount)
        if not wallet: return

        msg = await m.answer_dice(emoji="🎳")
        val = msg.dice.value
        await asyncio.sleep(4.0) # Bowling animation takes the longest

        if val == 6:
            multiplier, result_text = 3.0, "🎳 <b>PERFECT STRIKE! (3x)</b>"
        elif val == 5:
            multiplier, result_text = 1.5, "💥 <b>Great roll! (1.5x)</b>"
        else:
            multiplier, result_text = 0, "❌ <b>Gutter ball / Miss!</b>"

        if multiplier > 0:
            amt, currency = await handle_payout(p, int(amount * multiplier), amount)
            await m.answer(f"{result_text}\nYou won {amt:,} {currency}!", parse_mode="HTML")
        else:
            await handle_loss(p)
            currency = "Pub Coins" if p.get('in_pub') else "gold"
            await m.answer(f"{result_text}\nYou lost {amount:,} {currency}.", parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)


@dp.message(Command("basket"))
async def cmd_basket(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait for your current game to finish!")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit(): return await m.answer("❌ Usage: /basket [amount]")
    
    amount = int(args[1])
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        wallet = await handle_bet(m, p, "basket", amount)
        if not wallet: return

        msg = await m.answer_dice(emoji="🏀")
        val = msg.dice.value
        await asyncio.sleep(3.5) 

        if val == 5:
            multiplier, result_text = 2.5, "🏀 <b>CLEAN SWISH! (2.5x)</b>"
        elif val == 4:
            multiplier, result_text = 1.5, "⛹️ <b>Rim bounces in! (1.5x)</b>"
        else:
            multiplier, result_text = 0, "❌ <b>Brick! Missed the hoop.</b>"

        if multiplier > 0:
            amt, currency = await handle_payout(p, int(amount * multiplier), amount)
            await m.answer(f"{result_text}\nYou won {amt:,} {currency}!", parse_mode="HTML")
        else:
            await handle_loss(p)
            currency = "Pub Coins" if p.get('in_pub') else "gold"
            await m.answer(f"{result_text}\nYou lost {amount:,} {currency}.", parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)


@dp.message(Command("football"))
async def cmd_football(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait for your current game to finish!")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit(): return await m.answer("❌ Usage: /football [amount]")
    
    amount = int(args[1])
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        wallet = await handle_bet(m, p, "football", amount)
        if not wallet: return

        msg = await m.answer_dice(emoji="⚽")
        val = msg.dice.value
        await asyncio.sleep(3.5) 

        if val in [4, 5]:
            multiplier, result_text = 2.5, "⚽ <b>TOP CORNER GOAL! (2.5x)</b>"
        elif val == 3:
            multiplier, result_text = 1.5, "🥅 <b>Messy goal, but it counts! (1.5x)</b>"
        else:
            multiplier, result_text = 0, "❌ <b>Hit the post / Saved!</b>"

        if multiplier > 0:
            amt, currency = await handle_payout(p, int(amount * multiplier), amount)
            await m.answer(f"{result_text}\nYou won {amt:,} {currency}!", parse_mode="HTML")
        else:
            await handle_loss(p)
            currency = "Pub Coins" if p.get('in_pub') else "gold"
            await m.answer(f"{result_text}\nYou lost {amount:,} {currency}.", parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)


@dp.message(Command("dice"))
async def cmd_dice(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait for your current game to finish!")
    if m.from_user.id in dice_games: return await m.answer("❌ You have an active dice game! Play it first.")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit(): return await m.answer("❌ Usage: /dice [amount]")
    
    amount = int(args[1])
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user) 
        wallet = await handle_bet(m, p, "dice", amount)
        if not wallet: return
            
        dice_games[m.from_user.id] = {"bet": amount, "ts": datetime.now().timestamp()}

        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⬇️ Below 7 (2x)", callback_data=f"dice_low_{m.from_user.id}")],
            [types.InlineKeyboardButton(text="🎯 Lucky 7 (5x)", callback_data=f"dice_mid_{m.from_user.id}")],
            [types.InlineKeyboardButton(text="⬆️ Above 7 (2x)", callback_data=f"dice_high_{m.from_user.id}")]
        ])
        await m.answer(f"🎲 <b>Dice Under/Over</b>\nBet: {amount}\nSelect outcome:", reply_markup=kb, reply_to_message_id=m.message_id, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

@dp.callback_query(F.data.startswith("dice_"))
async def handle_dice(call: types.CallbackQuery):
    parts = call.data.split("_")
    mode = parts[1]
    user_id = int(parts[2])

    if call.from_user.id != user_id:
        return await call.answer("❌ This is not your game!", show_alert=True)
        
    if call.from_user.id in active_games: return await call.answer("⏳ Please wait for your current game to finish!", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        game = dice_games.pop(call.from_user.id, None)
        if not game: return await call.answer("❌ Game expired or already played.", show_alert=True)
        amount = game['bet']

        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        
        p = await get_player_fast(call.from_user)

        d1_msg = await call.message.answer_dice(emoji="🎲")
        d2_msg = await call.message.answer_dice(emoji="🎲")
        
        val1, val2 = d1_msg.dice.value, d2_msg.dice.value
        total = val1 + val2
        await asyncio.sleep(3.5)
        
        won = False
        multiplier = 0
        if mode == "low" and total < 7: won, multiplier = True, 2
        elif mode == "mid" and total == 7: won, multiplier = True, 5
        elif mode == "high" and total > 7: won, multiplier = True, 2

        await call.message.edit_text(f"🎲 Result: {val1} + {val2} = <b>{total}</b>", parse_mode="HTML")
        
        if won:
            amt, currency = await handle_payout(p, amount * multiplier, amount)
            await call.message.answer(f"✅ <b>Winner!</b>\nYou received {amt:,} {currency}!", parse_mode="HTML")
        else:
            await handle_loss(p)
            currency = "Pub Coins" if p.get('in_pub') else "gold"
            await call.message.answer(f"💀 <b>Lost!</b>\nHouse takes {amount:,} {currency}.", parse_mode="HTML")
    finally:
        active_games.discard(call.from_user.id)

@dp.message(Command("mines"))
async def cmd_mines(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    if m.from_user.id in mine_games: return await m.answer("❌ You already have an active minefield! Finish it or cash out first.")

    args = m.text.split()
    if len(args) < 3 or not args[1].isdigit() or not args[2].isdigit(): return await m.answer("❌ Usage: /mines [amount] [mines (1-24)]")

    amount, m_count = int(args[1]), int(args[2])
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        if not (1 <= m_count <= 24): return await m.answer("❌ Invalid mine count (1-24).")
        
        wallet = await handle_bet(m, p, "mines", amount)
        if not wallet: return

        all_pos = list(range(25))
        mines = random.sample(all_pos, m_count)
        
        mine_games[m.from_user.id] = {
            "mines": mines, 
            "found": 0, 
            "bet": amount, 
            "m_count": m_count, 
            "ts": datetime.now().timestamp(),
            "revealed": []
        }

        await m.answer(f"💣 <b>Mines: {m_count}</b> | Bet: {amount}\nPick a tile!", 
                       reply_markup=get_mines_kb(m.from_user.id), reply_to_message_id=m.message_id, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

def get_mines_kb(user_id, revealed=None, show_all=False, mines=None):
    if revealed is None: revealed = []
    buttons = []
    for i in range(25):
        text = "❓"
        if show_all:
            text = "💣" if i in mines else "💎"
        elif i in revealed:
            text = "💎"
        buttons.append(types.InlineKeyboardButton(text=text, callback_data=f"mine_{user_id}_hit_{i}"))
    
    kb = [buttons[i:i + 5] for i in range(0, 25, 5)]
    if not show_all and revealed:
        kb.append([types.InlineKeyboardButton(text="💰 Cash Out", callback_data=f"mine_{user_id}_cashout")])
    return types.InlineKeyboardMarkup(inline_keyboard=kb)

@dp.callback_query(F.data.startswith("mine_"))
async def handle_mine_click(call: types.CallbackQuery):
    parts = call.data.split("_")
    if len(parts) < 3: return
    user_id = int(parts[1])
    action = parts[2]

    if call.from_user.id != user_id:
        return await call.answer("❌ This is not your game!", show_alert=True)
        
    uid = call.from_user.id
    if uid not in mine_games: return await call.answer("No active game.", show_alert=True)
    
    if uid in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(uid)
    
    try:
        game = mine_games[uid]
        game['ts'] = datetime.now().timestamp() 
        
        if action == "cashout":
            mult = 1 + (game['found'] * (game['m_count'] / 10))
            p = await get_player_fast(call.from_user)
            amt, currency = await handle_payout(p, int(game['bet'] * mult), game['bet'])
            del mine_games[uid]
            await call.message.edit_text(f"💰 <b>Cashed Out!</b>\nYou won {amt:,} {currency}.", parse_mode="HTML")

        elif action == "hit":
            idx = int(parts[3])
            
            if idx in game['mines']:
                p = await get_player_fast(call.from_user) 
                await handle_loss(p)
                del mine_games[uid]
                await call.message.edit_text("💥 <b>BOOM!</b> You hit a mine.", reply_markup=get_mines_kb(uid, show_all=True, mines=game['mines']), parse_mode="HTML")

            elif idx not in game.get('revealed', []): 
                game.setdefault('revealed', []).append(idx)
                game['found'] += 1
                
                current_mult = round(1 + (game['found'] * (game['m_count'] / 10)), 2)
                potential_win = int(game['bet'] * current_mult)
                
                await call.message.edit_text(
                    f"💣 <b>Mines: {game['m_count']}</b> | Bet: {game['bet']}\n"
                    f"✨ <b>Current Multiplier: {current_mult}x</b>\n"
                    f"💰 Potential Win: {potential_win:,} coins\n\n"
                    "Pick another tile or Cash Out!",
                    reply_markup=get_mines_kb(uid, revealed=game['revealed']),
                    parse_mode="HTML"
                )
    finally:
        active_games.discard(uid)

@dp.message(Command("cards"))
async def cmd_cards(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait for your current game to finish!")
    if m.from_user.id in card_games: return await m.answer("❌ You already have an active card game! Finish it first.")
    
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit(): return await m.answer("❌ Usage: /cards [amount]")
    
    amount = int(args[1])
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        wallet = await handle_bet(m, p, "cards", amount)
        if not wallet: return

        card = random.choice(CARDS)
        card_games[m.from_user.id] = {"card": card, "bet": amount, "ts": datetime.now().timestamp()}
        
        # Build dynamic UI buttons showing exact payouts
        mults = CARD_MULTIPLIERS[card]
        h_text = f"⬆️ Higher ({mults['high']}x)" if mults['high'] > 0 else "⬆️ Higher (Auto-Lose)"
        l_text = f"⬇️ Lower ({mults['low']}x)" if mults['low'] > 0 else "⬇️ Lower (Auto-Lose)"
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text=h_text, callback_data=f"card_high_{m.from_user.id}"),
            types.InlineKeyboardButton(text=l_text, callback_data=f"card_low_{m.from_user.id}")
        ]])
        await m.answer(f"🃏 <b>Card: {card}</b>\nWill the next be higher or lower?", reply_markup=kb, reply_to_message_id=m.message_id, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

@dp.callback_query(F.data.startswith("card_"))
async def handle_cards(call: types.CallbackQuery):
    parts = call.data.split("_")
    choice = parts[1]
    user_id = int(parts[2])

    if call.from_user.id != user_id:
        return await call.answer("❌ This is not your game!", show_alert=True)
    
    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)

    try:
        game = card_games.pop(call.from_user.id, None)
        if not game: return await call.answer("❌ Game expired or already played.", show_alert=True)
            
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass 
        
        old_card = game['card']
        amount = game['bet']
        
        p = await get_player_fast(call.from_user)

        new_card = random.choice(CARDS)
        old_idx, new_idx = CARDS.index(old_card), CARDS.index(new_card)
        
        # Tie = Refund (Maintained your exact logic)
        if old_idx == new_idx:
            if p.get('in_pub'):
                p['pub_balance'] = p.get('pub_balance', 0) + amount
                queue_db_update("players", {"pub_balance": p['pub_balance']}, "id", p['id'])
                currency = "Pub Coins"
            else:
                await update_player_balance(p['id'], amount)
                currency = "gold"
                
            await call.message.edit_text(
                f"🃏 Old: {old_card} | New: <b>{new_card}</b>\n"
                f"🤝 <b>Draw!</b> Same card drawn. Your <b>{amount:,} {currency}</b> bet was refunded.", 
                parse_mode="HTML"
            )
        else:
            win = (choice == "high" and new_idx > old_idx) or (choice == "low" and new_idx < old_idx)
            multiplier = CARD_MULTIPLIERS[old_card][choice]
            
            # If they win and the multiplier is greater than 0
            if win and multiplier > 0:
                payout_amount = int(amount * multiplier)
                amt, currency = await handle_payout(p, payout_amount, amount)
                await call.message.edit_text(
                    f"🃏 Old: {old_card} | New: <b>{new_card}</b>\n"
                    f"✅ <b>Win! ({multiplier}x)</b> You received {amt:,} {currency}.", 
                    parse_mode="HTML"
                )
            else:
                await handle_loss(p)
                currency = "Pub Coins" if p.get('in_pub') else "gold"
                loss_reason = "Auto-Loss!" if multiplier == 0 else "Wrong guess!"
                await call.message.edit_text(
                    f"🃏 Old: {old_card} | New: <b>{new_card}</b>\n"
                    f"💀 <b>{loss_reason}</b> You lost {amount:,} {currency}.", 
                    parse_mode="HTML"
                )
    finally:
        active_games.discard(call.from_user.id)


def format_suffix(num):
    """Converts large numbers into clean K, M, B formats."""
    if num >= 1_000_000_000: return f"{num/1_000_000_000:.1f}B"
    if num >= 1_000_000: return f"{num/1_000_000:.1f}M"
    if num >= 10_000: return f"{num/1_000:.1f}K"
    return f"{num:,}" # Keeps commas for smaller balances

async def generate_top_text(user: types.User, mode: str = "wealth"):
    """Generates the minimal Top 5 leaderboard text for either Wealth or Level."""
    p = await get_player_fast(user)
    
    if mode == "wealth":
        res = await async_db(supabase.table("players").select("id, username, balance").order("balance", desc=True).limit(5))
        if not res.data: return "📭 The vault is currently empty."

        user_val = p['balance']
        rank_res = await async_db(supabase.table("players").select("id", count="exact").gt("balance", user_val))
        user_rank = (rank_res.count or 0) + 1

        text = "🏆 <b>𝗠𝗜𝗗𝗡𝗜𝗚𝗛𝗧 𝗧𝗢𝗣 𝟱: 𝗪𝗘𝗔𝗟𝗧𝗛</b> 💰\n━━━━━━━━━━━━━━━━━━\n"
        for i, player in enumerate(res.data):
            rank = i + 1
            bal = format_suffix(player['balance'])
            raw_name = html.escape(player.get('username') or f"User_{player['id']}")
            display_name = f"<a>{raw_name}</a>"
            text += f"【{rank}】 {display_name} — {bal} 💰\n"

        text += f"━━━━━━━━━━━━━━━━━━\n📊 Your Wealth Rank — 【{user_rank}】"
        return text

    elif mode == "level":
        res = await async_db(supabase.table("players").select("id, username, level, xp").order("xp", desc=True).limit(5))
        if not res.data: return "📭 The syndicate is currently empty."

        user_val = p.get('xp', 0)
        rank_res = await async_db(supabase.table("players").select("id", count="exact").gt("xp", user_val))
        user_rank = (rank_res.count or 0) + 1

        text = "🏆 <b>𝗠𝗜𝗗𝗡𝗜𝗚𝗛𝗧 𝗧𝗢𝗣 𝟱: 𝗣𝗥𝗘𝗦𝗧𝗜𝗚𝗘</b> 🎖️\n━━━━━━━━━━━━━━━━━━\n"
        for i, player in enumerate(res.data):
            rank = i + 1
            lvl = player.get('level', 1)
            xp = format_suffix(player.get('xp', 0))
            raw_name = html.escape(player.get('username') or f"User_{player['id']}")
            display_name = f"<a>{raw_name}</a>"
            text += f"【{rank}】 {display_name} — Lvl {lvl} ({xp} XP)\n"

        text += f"━━━━━━━━━━━━━━━━━━\n📊 Your Level Rank — 【{user_rank}】"
        return text

def get_top_kb(user_id: int, current_mode: str):
    """Generates the toggle buttons depending on the current tab."""
    if current_mode == "wealth":
        return types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="🎖️ View Top Levels", callback_data=f"top_v_level_{user_id}"),
            types.InlineKeyboardButton(text="Refresh 🌀", callback_data=f"top_v_wealth_{user_id}")
        ]])
    else:
        return types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="💰 View Top Wealth", callback_data=f"top_v_wealth_{user_id}"),
            types.InlineKeyboardButton(text="Refresh 🌀", callback_data=f"top_v_level_{user_id}")
        ]])

@dp.message(Command("top"))
async def cmd_top(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    
    try:
        # Default to the Wealth tab when typing the command
        text = await generate_top_text(m.from_user, "wealth")
        kb = get_top_kb(m.from_user.id, "wealth")
        await m.answer(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
        
    except Exception as e:
        logging.error(f"Error in /top: {e}")
        await m.answer("❌ Error fetching the leaderboard.")
    finally:
        active_games.discard(m.from_user.id)

@dp.callback_query(F.data.startswith("top_v_"))
async def handle_top_navigation(call: types.CallbackQuery):
    parts = call.data.split("_")
    mode = parts[2] # "wealth" or "level"
    owner_id = int(parts[3])
    
    if call.from_user.id != owner_id:
        return await call.answer("❌ You can't toggle someone else's leaderboard!", show_alert=True)
        
    if call.from_user.id in active_games: return await call.answer("⏳ Please wait...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        text = await generate_top_text(call.from_user, mode)
        kb = get_top_kb(call.from_user.id, mode)
        
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
            await call.answer("Leaderboard Updated! 🌀")
        except Exception:
            await call.answer("Already up to date! 🌀")
            
    finally:
        active_games.discard(call.from_user.id)

@dp.message(Command("refer"))
async def cmd_refer(m: types.Message):
    bot_username = (await bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start=ref_{m.from_user.id}"
    
    # --- FIXED COUNT LOGIC ---
    res = await async_db(supabase.table("players").select("id, username").eq("referred_by", m.from_user.id))
    all_referrals = res.data if res.data else []
    total_referrals = len(all_referrals)
    
    # 2. Slice for UI cleanliness (Limits to 15 names)
    display_referrals = all_referrals[:15]

    # 3. Build the Syndicate List Text
    ref_list_text = ""
    if total_referrals > 0:
        # 👇 CHANGE THIS VARIABLE TO PREVENT CHAT SPAM 👇
        for ref in display_referrals: 
            raw_name = html.escape(ref.get('username') or f"User_{ref['id']}")
            prefix = "@" if not raw_name.startswith("User_") else "👤 "
            ref_list_text += f"• <a href='tg://user?id={ref['id']}'>{prefix}{raw_name}</a>\n"
            
        if total_referrals > 15:
            ref_list_text += f"<i>...and {total_referrals - 15} more</i>\n"
  
    else:
        ref_list_text = "<i>No enforcers recruited yet.</i>"

    # 4. The New Sleek UI
    text = (
        "👥 <b>𝗥𝗘𝗙𝗘𝗥 & 𝗘𝗔𝗥𝗡</b>\n\n"
        "<blockquote>Invite friends.\n"
        "Earn 5% of every win.\n"
        "Forever.</blockquote>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📈 <b>Stats</b>\n"
        f"• Total Recruits: <b>{total_referrals}</b>\n\n"
        "🤝 <b>Your Syndicate:</b>\n"
        f"<blockquote>{ref_list_text.strip()}</blockquote>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "<b>🔗 Your Personal Link:</b>\n"
        f"<code>{ref_link}</code>"
    )
    
    # Create the Share Button with a pre-filled Telegram message
    share_url = f"https://t.me/share/url?url={ref_link}&text=Play%20Midnight%20Casino%20with%20me!%20%F0%9F%8E%B0"
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="Share your invite link 🔗", url=share_url)
    ]])
    
    # Added disable_web_page_preview=True to keep it clean
    await m.answer(text, reply_to_message_id=m.message_id, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)

@dp.message(Command("spin"))
async def cmd_spin(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait for your current spin to finish!")
    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        today = str(datetime.now(IST).date())
        
        if p.get('last_spin_date') != today:
            updates = {"last_spin_date": today, "has_used_ad_spin": False, "ad_spin_unlocked": False}
            p.update(updates)
            queue_db_update("players", updates, "id", m.from_user.id)
            
            await execute_spin(m, p)
            await asyncio.sleep(0.5)
            kb = await get_spin_ad_kb(m.chat.type, m.from_user.id)
            await m.answer("🎰 <b>Daily spin complete!</b>\nWatch an ad to unlock <b>one more spin!</b>", reply_markup=kb, parse_mode="HTML")
            return

        if p.get('ad_spin_unlocked'):
            p['ad_spin_unlocked'] = False
            p['has_used_ad_spin'] = True
            queue_db_update("players", {"ad_spin_unlocked": False, "has_used_ad_spin": True}, "id", m.from_user.id)
            await execute_spin(m, p)
            return

        kb = await get_spin_ad_kb(m.chat.type, m.from_user.id)
        if not p.get('has_used_ad_spin'):
            await m.answer("🎰 <b>Daily spin used.</b>\nWatch an ad to unlock <b>one more spin!</b>", reply_markup=kb, parse_mode="HTML")
        else:
            await m.answer("❌ <b>No spins left today!</b> Come back tomorrow.", reply_markup=kb, parse_mode="HTML")
            
    finally:
        active_games.discard(m.from_user.id)

async def execute_spin(m, p):
    msg = await m.answer_dice(emoji="🎰")
    await asyncio.sleep(3.5)
    
    val = msg.dice.value
    if val == 64: 
        reward = 5000
        text = "🎰 <b>JACKPOT (777)!</b>"
    elif val in [1, 22, 43]: 
        reward = 1500
        text = "✨ <b>TRIPLE!</b>"
    elif val % 2 == 0: 
        reward = 500
        text = "🍒 <b>Double!</b>"
    else: 
        reward = 100
        text = "🔹 Small win."
    
    await update_player_balance(m.from_user.id, reward)
    await m.answer(f"{text}\n🎉 You won <b>{reward}</b> coins!", parse_mode="HTML")

# --- GACHA & IDLE WAIFU SYSTEM ---
async def build_roster_ui(user: types.User, cmd_type: str, rarity: str, page: int):
    """Generates the interactive UI for both /harem and /characters."""
    p = await get_player_fast(user)
    owned_ids = p.get('owned_characters') or []
    
    # 1. Filter characters from the global RAM cache
    all_in_rarity = [c for c in CHARACTER_CACHE.values() if c['rarity'] == rarity]
    # Sort by ID to keep it consistent
    all_in_rarity.sort(key=lambda x: x['id']) 
    
    total_exists = len(all_in_rarity)
    
    if cmd_type == "harem":
        # Only keep the ones the player owns
        display_list = [c for c in all_in_rarity if c['id'] in owned_ids]
        header_title = f"🌸 <b>YOUR COLLECTION: @{html.escape(user.username or user.first_name)}</b>"
        count_text = f"Owned: {len(display_list)} / {total_exists}"
    else:
        # Show all of them (Admin DB)
        display_list = all_in_rarity
        header_title = "📚 <b>GLOBAL DATABASE</b>"
        count_text = f"Total in Database: {total_exists}"

    # 2. Pagination Math
    items_per_page = 10
    total_pages = math.ceil(len(display_list) / items_per_page) if display_list else 1
    
    # Safe fallback if page goes out of bounds
    if page >= total_pages: page = total_pages - 1
    if page < 0: page = 0
    
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_items = display_list[start_idx:end_idx]

    # 3. Build the Text
    e = EMOJIS.get(rarity, "✨")
    text = (
        f"{header_title}\n"
        f"{e} <b>{rarity.upper()} TIER</b> ({count_text})\n"
        f"━━━━━━━━━━━━━━━━━━\n"
    )
    
    if not page_items:
        text += "<i>No characters found in this tier.</i>\n"
    else:
        for c in page_items:
            # Clean layout: [ID] Name | Anime
            text += f"<code>[{str(c['id']).zfill(3)}]</code> {c['name']} | <i>{c['anime']}</i>\n"
            
    text += "━━━━━━━━━━━━━━━━━━"
    if cmd_type == "harem":
        text += "\n<i>Use /equip [id] [1/2/3] to draft them!</i>"

    # 4. Build the Dynamic Keyboard
    kb_rows = []
    
    # ROW 1: Pagination (Only shows if needed)
    nav_row = []
    if page > 0:
        nav_row.append(types.InlineKeyboardButton(text="⬅️ Prev", callback_data=f"nav_{cmd_type}_{user.id}_{rarity}_{page-1}"))
    
    if total_pages > 1:
        nav_row.append(types.InlineKeyboardButton(text=f"📄 {page+1}/{total_pages}", callback_data="noop"))
        
    if page < total_pages - 1:
        nav_row.append(types.InlineKeyboardButton(text="Next ➡️", callback_data=f"nav_{cmd_type}_{user.id}_{rarity}_{page+1}"))
        
    if nav_row:
        kb_rows.append(nav_row)

    # ROW 2: Rarity Tabs
    rarity_order = ["Common", "Rare", "Epic", "Legendary", "Mythic"]
    tab_row = []
    for r in rarity_order:
        r_emoji = EMOJIS.get(r, "")
        # Add brackets around the currently selected rarity so they know where they are
        btn_text = f"[{r_emoji}]" if r == rarity else r_emoji
        tab_row.append(types.InlineKeyboardButton(text=btn_text, callback_data=f"nav_{cmd_type}_{user.id}_{r}_0"))
        
    kb_rows.append(tab_row)

    return text, types.InlineKeyboardMarkup(inline_keyboard=kb_rows)


@dp.message(Command("harem", "collection"))
async def cmd_harem(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    try:
        # Default to Common page 0
        text, kb = await build_roster_ui(m.from_user, "harem", "Common", 0)
        await m.answer(text, reply_markup=kb, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)


@dp.message(Command("characters"))
async def cmd_characters(m: types.Message):
    # Security: Emperor Only
    if m.from_user.id != 7708811819: return await m.answer("❌ Unauthorized.")
        
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    try:
        # Default to Common page 0
        text, kb = await build_roster_ui(m.from_user, "chars", "Common", 0)
        await m.answer(text, reply_markup=kb, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)


@dp.callback_query(F.data.startswith("nav_"))
async def handle_roster_nav(call: types.CallbackQuery):
    parts = call.data.split("_")
    cmd_type = parts[1] # "harem" or "chars"
    owner_id = int(parts[2])
    rarity = parts[3]
    page = int(parts[4])

    if call.from_user.id != owner_id:
        return await call.answer("❌ This is not your menu!", show_alert=True)
        
    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        text, kb = await build_roster_ui(call.from_user, cmd_type, rarity, page)
        
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            await call.answer() # Silently clear the loading state
        except Exception:
            await call.answer("Already on this page! 🌀")
            
    finally:
        active_games.discard(call.from_user.id)

@dp.message(Command("summon"))
async def cmd_summon(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
    active_games.add(m.from_user.id)
    try:
        if not CHARACTER_CACHE:
            return await m.answer("❌ The Gacha pool is currently empty. Tell the Admin to add characters!")
            
        p = await get_player_fast(m.from_user)
        pity = p.get('pity_counter', 0)
        
        text = (
            f"🌸 <b>𝗦𝗬𝗡𝗗𝗜𝗖𝗔𝗧𝗘 𝗥𝗘𝗖𝗥𝗨𝗜𝗧𝗠𝗘𝗡𝗧</b> 🌸\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"✨ Pity Progress: <b>{pity} / 100</b>\n"
            f"<i>(100 Pity = Guaranteed Mythic!)</i>\n\n"
            f"Select a contract tier to view its drop rates and prices:"
        )
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🥉 Standard Contract", callback_data=f"sum_tier_{m.from_user.id}_standard")],
            [types.InlineKeyboardButton(text="🥈 Elite Contract", callback_data=f"sum_tier_{m.from_user.id}_elite")],
            [types.InlineKeyboardButton(text="🥇 Executive Contract", callback_data=f"sum_tier_{m.from_user.id}_executive")]
        ])
        await m.answer(text, reply_markup=kb, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

@dp.callback_query(F.data.startswith("sum_tier_"))
async def handle_summon_tier(call: types.CallbackQuery):
    parts = call.data.split("_")
    owner_id = int(parts[2])
    tier_key = parts[3]

    if call.from_user.id != owner_id:
        return await call.answer("❌ This is not your terminal!", show_alert=True)
        
    tier = SUMMON_TIERS[tier_key]
    
    # Format Drop Rates text
    rates_text = ""
    for rarity, chance in tier['rates'].items():
        rates_text += f"• {rarity} ({EMOJIS.get(rarity, '✨')}): <b>{chance}%</b>\n"
        
    text = (
        f"{tier['emoji']} <b>{tier['name'].upper()} CONTRACT</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Cost:</b> {tier['cost']:,} Gold per pull\n"
        f"{tier['desc']}\n\n"
        f"📊 <b>Drop Rates:</b>\n"
        f"{rates_text}\n"
        f"How many recruits do you want to draft?"
    )
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [
            types.InlineKeyboardButton(text=f"👤 1x Pull ({format_suffix(tier['cost'])})", callback_data=f"sum_pull_{owner_id}_{tier_key}_1"),
            types.InlineKeyboardButton(text=f"👥 10x Pull ({format_suffix(tier['cost']*10)})", callback_data=f"sum_pull_{owner_id}_{tier_key}_10")
        ],
        [types.InlineKeyboardButton(text="🔙 Back to Banners", callback_data=f"sum_back_{owner_id}")]
    ])
    
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("sum_back_"))
async def handle_summon_back(call: types.CallbackQuery):
    owner_id = int(call.data.split("_")[2])
    if call.from_user.id != owner_id: return await call.answer("❌ This is not your terminal!", show_alert=True)
    
    p = await get_player_fast(call.from_user)
    pity = p.get('pity_counter', 0)
    
    text = (
        f"🌸 <b>𝗦𝗬𝗡𝗗𝗜𝗖𝗔𝗧𝗘 𝗥𝗘𝗖𝗥𝗨𝗜𝗧𝗠𝗘𝗡𝗧</b> 🌸\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"✨ Pity Progress: <b>{pity} / 100</b>\n"
        f"<i>(100 Pity = Guaranteed Mythic!)</i>\n\n"
        f"Select a contract tier to view its drop rates and prices:"
    )
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🥉 Standard Contract", callback_data=f"sum_tier_{call.from_user.id}_standard")],
        [types.InlineKeyboardButton(text="🥈 Elite Contract", callback_data=f"sum_tier_{call.from_user.id}_elite")],
        [types.InlineKeyboardButton(text="🥇 Executive Contract", callback_data=f"sum_tier_{call.from_user.id}_executive")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("sum_pull_"))
async def handle_summon_pull(call: types.CallbackQuery):
    parts = call.data.split("_")
    owner_id = int(parts[2])
    tier_key = parts[3]
    amount = int(parts[4])

    if call.from_user.id != owner_id: return await call.answer("❌ This is not your terminal!", show_alert=True)
    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    
    active_games.add(call.from_user.id)
    try:
        tier = SUMMON_TIERS[tier_key]
        total_cost = tier['cost'] * amount
        
        p = await get_player_fast(call.from_user)
        if p['balance'] < total_cost:
            return await call.answer(f"❌ Insufficient Funds! You need {format_suffix(total_cost)} gold.", show_alert=True)
            
        # 1. Take the money immediately
        await update_player_balance(call.from_user.id, -total_cost)
        
        try:
            await call.message.edit_text("⏳ <i>Drafting from the Syndicate database...</i>", parse_mode="HTML")
        except: pass

        owned = p.get('owned_characters') or []
        pity = p.get('pity_counter', 0)
        pulls = []
        duplicate_gold = 0
        
        # --- THE PULL LOOP ---
        for _ in range(amount):
            pity += 1
            
            # HARD PITY CHECK (100)
            if pity >= 100:
                target_rarity = "Mythic"
                pity = 0 # Reset
            else:
                # Normal RNG Roll
                roll = random.uniform(0, 100)
                cumulative = 0.0
                target_rarity = "Common" # Fallback
                
                # We sort rates so we check the smallest % (Mythic) first!
                sorted_rates = sorted(tier['rates'].items(), key=lambda x: x[1])
                for rarity, chance in sorted_rates:
                    cumulative += chance
                    if roll <= cumulative:
                        target_rarity = rarity
                        break
                        
            # Soft Pity Reset Check
            if target_rarity in ["Legendary", "Mythic"]:
                pity = 0
                
            # Grab character from cache
            pool = [c for c in CHARACTER_CACHE.values() if c['rarity'] == target_rarity]
            if not pool: pool = list(CHARACTER_CACHE.values()) # Failsafe
            
            char = random.choice(pool)
            pulls.append(char)
            
            if char['id'] in owned:
                duplicate_gold += int(tier['cost'] * 0.2) # 20% refund per duplicate
            else:
                owned.append(char['id'])

        # --- UPDATE DATABASE ---
        updates = {"owned_characters": owned, "pity_counter": pity}
        if pulls and not p.get('last_collect_time'):
            updates['last_collect_time'] = datetime.now(IST).isoformat()
            
        await update_player_balance(call.from_user.id, duplicate_gold, updates)
        p['owned_characters'] = owned
        p['pity_counter'] = pity
        
        # --- UI OUTPUT ---
        if amount == 1:
            char = pulls[0]
            e = EMOJIS.get(char['rarity'], "✨")
            
            caption = f"{e} <b>{char['rarity'].upper()} PULL!</b> {e}\n\n🌸 <b>{char['name']}</b>\n📺 {char['anime']}\n💰 Base Income: {RARITY_RATES[char['rarity']]:,}/hr\n\n📈 Pity Progress: <b>{pity} / 100</b>"
            
            if duplicate_gold > 0:
                caption += f"\n\n⚠️ <i>You already own this character!</i>\n💸 Refunded: <b>{duplicate_gold:,} gold</b>"
            else:
                caption += "\n\n🎉 <b>NEW CHARACTER UNLOCKED!</b>"
                # Sys Log Alert for high tiers
                if char['rarity'] in ["Legendary", "Mythic"]:
                    safe_name = html.escape(call.from_user.first_name)
                    asyncio.create_task(send_log(f"🚨 <b>GACHA (NEW)</b>\n<a href='tg://user?id={call.from_user.id}'>{safe_name}</a> pulled {e} <b>{char['name']}</b> [{char['rarity']}] from the {tier['name']} Banner!"))

            await call.message.delete()
            await call.message.answer_photo(photo=char['image'], caption=caption, parse_mode="HTML")

        else:
            # 10x Pull Text Receipt
            text = (
                f"🎰 <b>10x {tier['name'].upper()} SUMMON</b> 🎰\n"
                f"━━━━━━━━━━━━━━━━━━\n"
            )
            
            # Sort pulls so best rarity is at the bottom for hype
            rarity_values = {"Common": 1, "Rare": 2, "Epic": 3, "Legendary": 4, "Mythic": 5}
            sorted_pulls = sorted(pulls, key=lambda x: rarity_values.get(x['rarity'], 0))
            
            for c in sorted_pulls:
                e = EMOJIS.get(c['rarity'], "✨")
                dupe_tag = " (Dupe)" if sorted_pulls.count(c) > 1 or c['id'] in p.get('owned_characters', [])[:-len(pulls)] else " ✨NEW✨"
                # Strip out the NEW tag if it's a dupe to keep it clean
                if "Dupe" in dupe_tag: dupe_tag = " <i>(Dupe)</i>"
                text += f"{e} [{c['rarity']}] <b>{c['name']}</b>{dupe_tag}\n"
                
            text += f"━━━━━━━━━━━━━━━━━━\n📈 Pity Progress: <b>{pity} / 100</b>\n"
            if duplicate_gold > 0:
                text += f"💸 Duplicates Refunded: <b>+{duplicate_gold:,} Gold</b>\n"

            # Check if there's a good pull to show off in the image
            best_char = sorted_pulls[-1]
            if rarity_values.get(best_char['rarity'], 0) >= 3: # Epic or better
                await call.message.delete()
                await call.message.answer_photo(photo=best_char['image'], caption=text, parse_mode="HTML")
            else:
                await call.message.edit_text(text, parse_mode="HTML")

    finally:
        active_games.discard(call.from_user.id)


@dp.message(Command("view"))
async def cmd_view(m: types.Message):
    args = m.text.split()
    
    if len(args) < 2 or not args[1].isdigit():
        return await m.answer(
            "❌ <b>Usage:</b>\n`/view [id]`\n<i>Find the ID in your /harem or ask the Admin!</i>", 
            parse_mode="Markdown"
        )
        
    char_id = int(args[1])
    
    # Check RAM Cache directly instead of the user's owned list
    if char_id not in CHARACTER_CACHE:
        return await m.answer("❌ This character does not exist in the database.", parse_mode="HTML")
        
    char = CHARACTER_CACHE[char_id]
    p = await get_player_fast(m.from_user)
    owned = p.get('owned_characters') or []
    
    # Show if they own it or not
    ownership_status = "✅ <b>In Collection</b>" if char_id in owned else "🔒 <b>Not Owned</b>"
    
    emojis = {"Common": "🤍", "Rare": "💙", "Epic": "💜", "Legendary": "💛", "Mythic": "💖"}
    e = emojis.get(char['rarity'], "✨")
    rate = RARITY_RATES.get(char['rarity'], 0)
    
    caption = (
        f"{e} <b>{char['name']}</b> {e}\n\n"
        f"📺 <b>Anime:</b> {char['anime']}\n"
        f"✨ <b>Rarity:</b> {char['rarity']}\n"
        f"💰 <b>Passive Income:</b> +{rate:,} gold/hr\n\n"
        f"📦 <b>Status:</b> {ownership_status}\n"
    )
    
    if char_id in owned:
        caption += f"\n<i>Use /equip {char_id} [1/2/3] to add them to your team!</i>"
    else:
        caption += "\n<i>Use /summon to try and pull this character!</i>"
        
    await m.answer_photo(photo=char['image'], caption=caption, parse_mode="HTML")


@dp.message(Command("delchar"))
async def cmd_delchar(m: types.Message):
    # Security: Emperor Only
    if m.from_user.id != 7708811819:
        return await m.answer("❌ Unauthorized.")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit():
        return await m.answer(
            "❌ <b>Usage:</b>\n`/delchar [id]`\n<i>Use /characters to find the ID.</i>", 
            parse_mode="HTML"
        )
        
    char_id = int(args[1])
    
    # Check if they exist in RAM
    if char_id not in CHARACTER_CACHE:
        return await m.answer(f"❌ Character ID <code>{char_id}</code> not found in the database.", parse_mode="HTML")
        
    char_name = CHARACTER_CACHE[char_id]['name']
    
    try:
        # 1. Delete from Supabase Database
        await async_db(supabase.table("characters").delete().eq("id", char_id))
        
        # 2. Delete from RAM Cache
        del CHARACTER_CACHE[char_id]
        
        await m.answer(
            f"🗑️ <b>CHARACTER DELETED</b> 🗑️\n\n"
            f"<b>{char_name}</b> (ID: <code>{char_id}</code>) has been permanently wiped from the Gacha pool and all player inventories.", 
            parse_mode="HTML"
        )
        
    except Exception as e:
        await m.answer(f"❌ <b>Database Error:</b>\n{str(e)}", parse_mode="HTML")

@dp.message(Command("team"))
async def cmd_team(m: types.Message):
    p = await get_player_fast(m.from_user)
    t1, t2, t3 = p.get('team_1'), p.get('team_2'), p.get('team_3')
    
    text = "⚔️ <b>YOUR ACTIVE TEAM</b> ⚔️\n\n"
    total_rate = 0
    
    for i, cid in enumerate([t1, t2, t3], 1):
        if cid and cid in CHARACTER_CACHE:
            c = CHARACTER_CACHE[cid]
            rate = RARITY_RATES.get(c['rarity'], 0)
            total_rate += rate
            text += f"<b>Slot {i}:</b> {c['name']} (+{rate:,}/hr)\n"
        else:
            text += f"<b>Slot {i}:</b> [Empty]\n"
            
    text += f"\n💰 <b>Total Generation:</b> {total_rate:,} gold / hour\n"
    text += "<i>Use /collect to claim your background earnings!</i>"
    
    await m.answer(text, parse_mode="HTML")

@dp.message(Command("equip"))
async def cmd_equip(m: types.Message):
    p = await get_player_fast(m.from_user)
    args = m.text.split()
    
    if len(args) < 3 or not args[1].isdigit() or not args[2].isdigit():
        return await m.answer("❌ Usage: `/equip [character_id] [slot 1/2/3]`", parse_mode="Markdown")
        
    cid = int(args[1])
    slot = int(args[2])
    
    owned = p.get('owned_characters') or []
    if cid not in owned:
        return await m.answer("❌ You don't own this character! Check your /harem.")
        
    if slot not in [1, 2, 3]:
        return await m.answer("❌ Slot must be 1, 2, or 3.")
        
    # Prevent equipping the same character in multiple slots
    if cid in [p.get('team_1'), p.get('team_2'), p.get('team_3')]:
        return await m.answer("❌ This character is already on your team!")
        
    # Update Slot
    col_name = f"team_{slot}"
    p[col_name] = cid
    queue_db_update("players", {col_name: cid}, "id", m.from_user.id)
    
    char = CHARACTER_CACHE[cid]
    await m.answer(f"✅ <b>{char['name']}</b> has been equipped to Slot {slot}!", parse_mode="HTML")

@dp.message(Command("collect"))
async def cmd_collect(m: types.Message):
    if m.from_user.id in active_games: return await m.answer("⏳ Wait...")
    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        last_time_str = p.get('last_collect_time')
        
        if not last_time_str:
            return await m.answer("❌ You need to /summon a character first to start generating gold!")
            
        t1, t2, t3 = p.get('team_1'), p.get('team_2'), p.get('team_3')
        if not any([t1, t2, t3]):
            return await m.answer("❌ You don't have any characters equipped! Use /team and /equip.")
            
        # Calculate Time Passed
        last_time = datetime.fromisoformat(last_time_str.replace('Z', '+00:00'))
        now = datetime.now(IST)
        
        # Make both timezone-aware for safe subtraction
        if last_time.tzinfo is None: last_time = last_time.replace(tzinfo=timezone.utc)
        
        hours_passed = (now - last_time).total_seconds() / 3600.0
        
        if hours_passed < 0.1: # Must wait at least 6 minutes
            return await m.answer("⏳ Your team is still working. Check back later!")
            
        # Format Time
        h = int(hours_passed)
        mins = int((hours_passed * 60) % 60)
        time_str = f"{h}h {mins}m"
        
        # Build the Enforcer Breakdown UI
        text = (
            "🏦 <b>𝗦𝗬𝗡𝗗𝗜𝗖𝗔𝗧𝗘 𝗥𝗘𝗩𝗘𝗡𝗨𝗘</b> 🏦\n"
            f"<i>Time Elapsed: {time_str}</i>\n"
            "━━━━━━━━━━━━━━━━━━\n"
        )
        
        total_generated = 0
        
        for cid in [t1, t2, t3]:
            if cid and cid in CHARACTER_CACHE:
                c = CHARACTER_CACHE[cid]
                rate = RARITY_RATES.get(c['rarity'], 0)
                earned = int(hours_passed * rate)
                total_generated += earned
                
                e = EMOJIS.get(c['rarity'], "✨")
                text += f"{e} <b>[{c['rarity']}] {c['name']}</b> generated <b>{earned:,}</b> 💰\n"
                
        text += (
            "\n💸 <b>Total Ready to Claim:</b> <b>{:,} Gold</b>\n"
            "━━━━━━━━━━━━━━━━━━"
        ).format(total_generated)
        
        # The Lock: Button is tied to their User ID!
        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="💰 Claim Loot", callback_data=f"collect_claim_{m.from_user.id}")
        ]])
        
        await m.answer(text, reply_markup=kb, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

# --- THE NEW CLAIM BUTTON HANDLER ---
@dp.callback_query(F.data.startswith("collect_claim_"))
async def handle_collect_claim(call: types.CallbackQuery):
    owner_id = int(call.data.split("_")[2])
    
    # 1. Block anyone else from clicking the button
    if call.from_user.id != owner_id:
        return await call.answer("❌ You can't claim someone else's Syndicate loot!", show_alert=True)
        
    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        p = await get_player_fast(call.from_user)
        last_time_str = p.get('last_collect_time')
        if not last_time_str: return await call.answer("❌ Error: No collection time found.", show_alert=True)
        
        # 2. Re-calculate the exact gold at the moment they clicked (Prevents exploits!)
        last_time = datetime.fromisoformat(last_time_str.replace('Z', '+00:00'))
        now = datetime.now(IST)
        if last_time.tzinfo is None: last_time = last_time.replace(tzinfo=timezone.utc)
        
        hours_passed = (now - last_time).total_seconds() / 3600.0
        
        t1, t2, t3 = p.get('team_1'), p.get('team_2'), p.get('team_3')
        total_generated = 0
        for cid in [t1, t2, t3]:
            if cid and cid in CHARACTER_CACHE:
                total_generated += int(hours_passed * RARITY_RATES.get(CHARACTER_CACHE[cid]['rarity'], 0))
                
        if total_generated <= 0:
            return await call.answer("❌ Nothing to claim right now.", show_alert=True)
            
        # 3. Securely update the database
        now_str = now.isoformat()
        await update_player_balance(call.from_user.id, total_generated, {"last_collect_time": now_str})
        p['last_collect_time'] = now_str
        
        # 4. Edit the message to show success
        await call.message.edit_text(
            f"✅ <b>LOOT SECURED!</b>\n\n"
            f"Your enforcers successfully deposited <b>{total_generated:,} gold</b> into your vault.",
            parse_mode="HTML"
        )
    finally:
        active_games.discard(call.from_user.id)

# ==========================================
# --- 14. PVP CHALLENGE HUB (THE ARENA) ---
# ==========================================

@dp.message(Command("challenge"))
async def cmd_challenge(m: types.Message):
    if not m.reply_to_message: return await m.answer("❌ You must reply to the user you want to challenge!")
    
    target = m.reply_to_message.from_user
    if target.id == m.from_user.id: return await m.answer("❌ You cannot challenge yourself.")
    if target.is_bot: return await m.answer("❌ You cannot challenge a bot.")

    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit(): return await m.answer("❌ <b>Usage:</b> `/challenge [amount]`", parse_mode="HTML")
    
    amount = int(args[1])
    if amount < 100: return await m.answer("❌ Minimum challenge amount is 100 Gold.")

    if m.from_user.id in active_games: return await m.answer("⏳ Please wait, you are busy...")

    active_games.add(m.from_user.id)
    try:
        p_c = await get_player_fast(m.from_user)
        p_d = await get_player_fast(target)
        
        if amount > p_c['balance']: return await m.answer("❌ Insufficient vault balance.")
        if amount > p_d['balance']: return await m.answer("❌ Opponent does not have enough gold.")

        # Temporarily store the setup state
        challenge_games[m.from_user.id] = {
            "challenger": m.from_user.id,
            "defender": target.id,
            "bet": amount,
            "c_name": html.escape(m.from_user.first_name),
            "d_name": html.escape(target.first_name),
            "status": "selecting",
            "ts": datetime.now().timestamp()
        }

        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [
                types.InlineKeyboardButton(text="🪙 Coin", callback_data=f"ch_sel_{m.from_user.id}_coin"),
                types.InlineKeyboardButton(text="🎲 Dice", callback_data=f"ch_sel_{m.from_user.id}_dice"),
                types.InlineKeyboardButton(text="🃏 Cards", callback_data=f"ch_sel_{m.from_user.id}_cards")
            ],
            [
                types.InlineKeyboardButton(text="🎯 Darts", callback_data=f"ch_sel_{m.from_user.id}_dart"),
                types.InlineKeyboardButton(text="🎳 Bowl", callback_data=f"ch_sel_{m.from_user.id}_bowl"),
                types.InlineKeyboardButton(text="🏀 Basket", callback_data=f"ch_sel_{m.from_user.id}_basket")
            ],
            [
                types.InlineKeyboardButton(text="⚽ Footy", callback_data=f"ch_sel_{m.from_user.id}_foot"),
                types.InlineKeyboardButton(text="💣 Mines", callback_data=f"ch_sel_{m.from_user.id}_mines"),
                types.InlineKeyboardButton(text="📦 Crates", callback_data=f"ch_sel_{m.from_user.id}_crates")
            ],
            [types.InlineKeyboardButton(text="❌ Cancel", callback_data=f"ch_cancel_{m.from_user.id}")]
        ])
        
        await m.answer(
            f"⚔️ <b>𝗖𝗛𝗢𝗢𝗦𝗘 𝗬𝗢𝗨𝗥 𝗪𝗘𝗔𝗣𝗢𝗡</b> ⚔️\n"
            f"<i>You are challenging @{html.escape(target.first_name)} for {amount:,} Gold.</i>",
            reply_markup=kb, parse_mode="HTML"
        )
    finally:
        active_games.discard(m.from_user.id)

@dp.callback_query(F.data.startswith("ch_sel_"))
async def handle_challenge_selection(call: types.CallbackQuery):
    parts = call.data.split("_")
    owner_id = int(parts[2])
    game_choice = parts[3]

    if call.from_user.id != owner_id: return await call.answer("❌ This is not your menu!", show_alert=True)
    
    game = challenge_games.get(owner_id)
    if not game or game['status'] != 'selecting': return await call.answer("❌ Challenge expired.", show_alert=True)

    game['game_type'] = game_choice
    game['status'] = 'pending'
    game['ts'] = datetime.now().timestamp()

    game_emojis = {"coin": "🪙 Coin Flip", "dice": "🎲 Dice Roll", "cards": "🃏 Cards", "dart": "🎯 Darts", "bowl": "🎳 Bowling", "basket": "🏀 Basketball", "foot": "⚽ Football", "mines": "💣 Minefield", "crates": "📦 Crates"}

    kb = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="⚔️ Accept", callback_data=f"ch_acc_{owner_id}"),
        types.InlineKeyboardButton(text="❌ Walk Away", callback_data=f"ch_dec_{owner_id}")
    ]])

    await call.message.edit_text(
        f"⚔️ <b>𝗧𝗛𝗘 𝗧𝗛𝗥𝗢𝗪𝗗𝗢𝗪𝗡</b> ⚔️\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🩸 <b>Challenger:</b> @{game['c_name']}\n"
        f"🎯 <b>Target:</b> <a href='tg://user?id={game['defender']}'>@{game['d_name']}</a>\n\n"
        f"🎮 <b>Game:</b> {game_emojis.get(game_choice)}\n"
        f"💰 <b>Wager:</b> {game['bet']:,} Gold\n"
        f"🏆 <b>Total Pot:</b> {game['bet'] * 2:,} Gold\n\n"
        f"<i>Do you accept the challenge, @{game['d_name']}?</i>",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("ch_cancel_"))
async def handle_challenge_cancel(call: types.CallbackQuery):
    owner_id = int(call.data.split("_")[2])
    if call.from_user.id != owner_id: return await call.answer("❌ Not yours!", show_alert=True)
    if owner_id in challenge_games: del challenge_games[owner_id]
    await call.message.edit_text("❌ <i>Challenge cancelled.</i>", parse_mode="HTML")

@dp.callback_query(F.data.startswith("ch_dec_"))
async def handle_challenge_decline(call: types.CallbackQuery):
    owner_id = int(call.data.split("_")[2])
    game = challenge_games.get(owner_id)
    if not game: return await call.answer("❌ Expired.", show_alert=True)
    
    if call.from_user.id not in [game['challenger'], game['defender']]:
        return await call.answer("❌ Not your fight!", show_alert=True)

    del challenge_games[owner_id]
    await call.message.edit_text(f"❌ <b>Challenge Declined.</b>\n@{game['d_name']} walked away.", parse_mode="HTML")

@dp.callback_query(F.data.startswith("ch_acc_"))
async def handle_challenge_accept(call: types.CallbackQuery):
    owner_id = int(call.data.split("_")[2])
    game = challenge_games.get(owner_id)
    
    if not game or game['status'] != 'pending': return await call.answer("❌ Expired.", show_alert=True)
    if call.from_user.id != game['defender']: return await call.answer("❌ You are not the target!", show_alert=True)

    if call.from_user.id in active_games or owner_id in active_games:
        return await call.answer("⏳ Processing...", show_alert=True)
        
    active_games.add(call.from_user.id)
    active_games.add(owner_id)
    
    try:
        p_c = await get_player_fast(types.User(id=owner_id, is_bot=False, first_name=""))
        p_d = await get_player_fast(call.from_user)

        if game['bet'] > p_c['balance'] or game['bet'] > p_d['balance']:
            del challenge_games[owner_id]
            return await call.message.edit_text("❌ Someone ran out of gold! Challenge voided.")

        # 💸 DEDUCT ESCROW FROM BOTH PLAYERS 💸
        await update_player_balance(owner_id, -game['bet'])
        await update_player_balance(call.from_user.id, -game['bet'])
        
        game['status'] = 'playing'
        game['ts'] = datetime.now().timestamp()
        
        await call.message.edit_text(f"⏳ <i>Locking in the {game['bet']*2:,} gold pot...</i>", parse_mode="HTML")
        
        # ---> ROUTING TO SPECIFIC GAMES WILL GO HERE <---
        await route_pvp_game(call.message, game)
        
    finally:
        active_games.discard(call.from_user.id)
        active_games.discard(owner_id)

async def pvp_afk_timer(owner_id: int, expected_ts: float):
    """Prevents players from holding the pot hostage if they close the app."""
    await asyncio.sleep(60)
    game = challenge_games.get(owner_id)
    
    # THE FIX: We check if expected_ts matches, meaning they didn't start a NEW game!
    if game and game.get('ts') == expected_ts and game.get('status') == 'playing' and not game.get('choice_made'):
        game['choice_made'] = True # Lock out manual clicks
        
        try:
            await bot.edit_message_reply_markup(chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=None)
            await bot.send_message(game['chat_id'], f"⏰ <b>TIME'S UP!</b>\n@{game['d_name']} took too long to decide. The House forces a random choice!", parse_mode="HTML")
        except: pass
        
        # Force a random execution based on the game type
        if game['game_type'] == 'coin':
            await execute_pvp_coin(owner_id, random.choice(['heads', 'tails']))
        elif game['game_type'] in ['dice', 'cards']:
            await execute_pvp_rng(owner_id, random.choice(['high', 'low']))


async def route_pvp_game(msg: types.Message, game: dict):
    bot_msg = None
    g_type = game['game_type']
    
    # 1. INSTANT SPORTS (No choice needed, directly to execution)
    if g_type in ['dart', 'bowl', 'basket', 'foot']:
        await execute_pvp_sports(msg, game)
        return

    # 2. COIN FLIP (Heads or Tails)
    elif g_type == 'coin':
        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="🪙 Heads", callback_data=f"pvp_ch_coin_{game['challenger']}_heads"),
            types.InlineKeyboardButton(text="🪙 Tails", callback_data=f"pvp_ch_coin_{game['challenger']}_tails")
        ]])
        bot_msg = await msg.answer(
            f"🪙 <b>𝗧𝗛𝗘 𝗖𝗢𝗜𝗡 𝗙𝗟𝗜𝗣</b> 🪙\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"The House tosses the coin to <a href='tg://user?id={game['defender']}'>@{game['d_name']}</a>...\n\n"
            f"👉 <b>Call it in the air!</b>",
            reply_markup=kb, parse_mode="HTML"
        )

    # 3. DICE & CARDS (Highest or Lowest)
    elif g_type in ['dice', 'cards']:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="⬆️ Highest Wins", callback_data=f"pvp_ch_rule_{game['challenger']}_high"),
            types.InlineKeyboardButton(text="⬇️ Lowest Wins", callback_data=f"pvp_ch_rule_{game['challenger']}_low")
        ]])
        bot_msg = await msg.answer(
            f"⚖️ <b>𝗧𝗛𝗘 𝗥𝗨𝗟𝗘𝗦</b> ⚖️\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"The House looks to <a href='tg://user?id={game['defender']}'>@{game['d_name']}</a> to set the terms...\n\n"
            f"👉 <b>How do we play?</b>",
            reply_markup=kb, parse_mode="HTML"
        )
        
    # 4. THE MINEFIELD
    elif g_type == 'mines':
        await setup_pvp_mines(msg, game)
        
    # 5. THE SMUGGLER'S SPLIT (CRATES)
    elif g_type == 'crates':
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [
                types.InlineKeyboardButton(text="📦 5", callback_data=f"pvp_cr_set_{game['challenger']}_5"),
                types.InlineKeyboardButton(text="📦 6", callback_data=f"pvp_cr_set_{game['challenger']}_6"),
                types.InlineKeyboardButton(text="📦 7", callback_data=f"pvp_cr_set_{game['challenger']}_7")
            ],
            [
                types.InlineKeyboardButton(text="📦 8", callback_data=f"pvp_cr_set_{game['challenger']}_8"),
                types.InlineKeyboardButton(text="📦 9", callback_data=f"pvp_cr_set_{game['challenger']}_9")
            ]
        ])
        bot_msg = await msg.answer(
            f"📦 <b>𝗧𝗛𝗘 𝗦𝗠𝗨𝗚𝗚𝗟𝗘𝗥'𝗦 𝗦𝗣𝗟𝗜𝗧</b> 📦\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"The Bouncer looks at <a href='tg://user?id={game['challenger']}'>@{game['c_name']}</a>...\n\n"
            f"👉 <b>How many crates are we dropping?</b>",
            reply_markup=kb, parse_mode="HTML"
        )

    if bot_msg:
        game['msg_id'] = bot_msg.message_id
        game['chat_id'] = bot_msg.chat.id
        # THE FIX: Pass the exact timestamp of this match into the timer!
        asyncio.create_task(pvp_afk_timer(game['challenger'], game['ts']))

async def execute_pvp_sports(msg: types.Message, game: dict):
    g_type = game['game_type']
    c_id = game['challenger']
    d_id = game['defender']
    
    # Map game type to the correct native Telegram emoji
    emojis = {"dart": "🎯", "bowl": "🎳", "basket": "🏀", "foot": "⚽"}
    emoji = emojis[g_type]
    
    await msg.answer(f"🔥 <i>The arena clears for @{game['c_name']} and @{game['d_name']}...</i>", parse_mode="HTML")
    await asyncio.sleep(1.5)
    
    # 1. Challenger Rolls
    await msg.answer(f"🩸 <b>@{game['c_name']}'s turn:</b>", parse_mode="HTML")
    c_msg = await msg.answer_dice(emoji=emoji)
    await asyncio.sleep(4.0) # Wait for animation to finish
    
    # 2. Defender Rolls
    await msg.answer(f"🎯 <b>@{game['d_name']}'s turn:</b>", parse_mode="HTML")
    d_msg = await msg.answer_dice(emoji=emoji)
    await asyncio.sleep(4.0) 
    
    c_val = c_msg.dice.value
    d_val = d_msg.dice.value
    
    # 3. Determine Winner
    if c_val > d_val:
        winner, loser = c_id, d_id
        win_name = game['c_name']
    elif d_val > c_val:
        winner, loser = d_id, c_id
        win_name = game['d_name']
    else:
        winner = None # Tie!
        
    pot = game['bet'] * 2

    # 4. Payouts and Logging
    if winner:
        tax = int(pot * 0.05) # 5% House Tax
        payout = pot - tax
        
        p_win = await get_player_fast(types.User(id=winner, is_bot=False, first_name=""))
        await update_player_balance(winner, payout, {"wins": p_win.get('wins', 0) + 1})
        
        p_lose = await get_player_fast(types.User(id=loser, is_bot=False, first_name=""))
        await handle_loss(p_lose) 
        
        # --- ADD THE AUDIT LOG HERE ---
        asyncio.create_task(send_log(f"🏟️ <b>PVP {g_type.upper()}</b>\n<a href='tg://user?id={winner}'>{win_name}</a> beat their opponent and took a <b>{payout:,} Gold</b> pot!"))
        
        await msg.answer(
            f"👑 <b>WINNER: @{win_name}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"They take home the <b>{payout:,} Gold</b> pot!\n"
            f"<i>(5% House Tax Applied)</i>", parse_mode="HTML"
        )
    else:
        # Absolute Tie - Refund Escrow safely
        await update_player_balance(c_id, game['bet'])
        await update_player_balance(d_id, game['bet'])
        await msg.answer(
            f"🤝 <b>IT'S A DRAW!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Both players rolled a {c_val}! The <b>{game['bet']:,} Gold</b> escrow has been refunded to both vaults.", parse_mode="HTML"
        )
        
    # Clean up the RAM
    del challenge_games[c_id]

@dp.callback_query(F.data.startswith("pvp_ch_coin_"))
async def handle_pvp_coin_choice(call: types.CallbackQuery):
    parts = call.data.split("_")
    c_id = int(parts[3])
    choice = parts[4]
    
    game = challenge_games.get(c_id)
    if not game or game['status'] != 'playing': return await call.answer("❌ Game expired.", show_alert=True)
    if call.from_user.id != game['defender']: return await call.answer("❌ The House asked the target, not you!", show_alert=True)
    if game.get('choice_made'): return await call.answer("⏳ Choice already locked in.", show_alert=True)
    
    game['choice_made'] = True
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except: pass
    
    await execute_pvp_coin(c_id, choice)

@dp.callback_query(F.data.startswith("pvp_ch_rule_"))
async def handle_pvp_rule_choice(call: types.CallbackQuery):
    parts = call.data.split("_")
    c_id = int(parts[3])
    rule = parts[4]
    
    game = challenge_games.get(c_id)
    if not game or game['status'] != 'playing': return await call.answer("❌ Game expired.", show_alert=True)
    if call.from_user.id != game['defender']: return await call.answer("❌ The House asked the target, not you!", show_alert=True)
    if game.get('choice_made'): return await call.answer("⏳ Rule already locked in.", show_alert=True)
    
    game['choice_made'] = True
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except: pass
    
    await execute_pvp_rng(c_id, rule)

async def execute_pvp_coin(c_id: int, d_choice: str):
    game = challenge_games.get(c_id)
    if not game: return
    chat_id = game['chat_id']
    
    c_choice = "tails" if d_choice == "heads" else "heads"
    await bot.send_message(
        chat_id, 
        f"🪙 <b>@{game['d_name']}</b> called <b>{d_choice.upper()}</b>!\n"
        f"By default, <b>@{game['c_name']}</b> takes <b>{c_choice.upper()}</b>.\n\n"
        f"✨ <i>The coin flips through the air...</i>", 
        parse_mode="HTML"
    )
    await asyncio.sleep(2.0)
    
    result = random.choice(["heads", "tails"])
    
    if result == d_choice:
        winner, loser, win_name = game['defender'], c_id, game['d_name']
    else:
        winner, loser, win_name = c_id, game['defender'], game['c_name']
        
    pot = game['bet'] * 2
    tax = int(pot * 0.05)
    payout = pot - tax
    
    p_win = await get_player_fast(types.User(id=winner, is_bot=False, first_name=""))
    await update_player_balance(winner, payout, {"wins": p_win.get('wins', 0) + 1})
    
    p_lose = await get_player_fast(types.User(id=loser, is_bot=False, first_name=""))
    await handle_loss(p_lose) 
    
    # --- ADD THE AUDIT LOG HERE ---
    asyncio.create_task(send_log(f"🪙 <b>PVP COIN FLIP</b>\n<a href='tg://user?id={winner}'>{win_name}</a> won the toss and took a <b>{payout:,} Gold</b> pot!"))
    
    await bot.send_message(
        chat_id,
        f"💥 It lands on <b>{result.upper()}</b>!\n\n"
        f"👑 <b>WINNER: @{win_name}!</b>\n"
        f"They take home the <b>{payout:,} Gold</b> pot!\n"
        f"<i>(5% House Tax Applied)</i>", parse_mode="HTML"
    )
    del challenge_games[c_id]

async def execute_pvp_rng(c_id: int, rule: str):
    game = challenge_games.get(c_id)
    if not game: return
    chat_id = game['chat_id']
    g_type = game['game_type']
    
    rule_text = "Highest Wins" if rule == "high" else "Lowest Wins"
    await bot.send_message(chat_id, f"⚖️ <b>Rule Locked:</b> {rule_text}!\n<i>The House deals...</i>", parse_mode="HTML")
    await asyncio.sleep(1.5)
    
    if g_type == 'dice':
        c_msg = await bot.send_dice(chat_id, emoji="🎲")
        d_msg = await bot.send_dice(chat_id, emoji="🎲")
        await asyncio.sleep(4.0)
        c_val, d_val = c_msg.dice.value, d_msg.dice.value
        c_disp, d_disp = f"🎲 {c_val}", f"🎲 {d_val}"
    else:
        # Cards
        c_card, d_card = random.choice(CARDS), random.choice(CARDS)
        c_val, d_val = CARDS.index(c_card), CARDS.index(d_card)
        c_disp, d_disp = f"🃏 {c_card}", f"🃏 {d_card}"
        await bot.send_message(
            chat_id, 
            f"🩸 <b>@{game['c_name']} draws:</b> {c_disp}\n"
            f"🎯 <b>@{game['d_name']} draws:</b> {d_disp}", 
            parse_mode="HTML"
        )
        await asyncio.sleep(1.0)
        
    if c_val == d_val:
        winner = None
    else:
        if rule == "high":
            winner = c_id if c_val > d_val else game['defender']
        else:
            winner = c_id if c_val < d_val else game['defender']
            
    win_name = game['c_name'] if winner == c_id else game['d_name']
    loser = game['defender'] if winner == c_id else c_id
    
    if winner:
        pot = game['bet'] * 2
        tax = int(pot * 0.05)
        payout = pot - tax
        
        p_win = await get_player_fast(types.User(id=winner, is_bot=False, first_name=""))
        await update_player_balance(winner, payout, {"wins": p_win.get('wins', 0) + 1})
        
        p_lose = await get_player_fast(types.User(id=loser, is_bot=False, first_name=""))
        await handle_loss(p_lose) 
        
        # --- ADD THE AUDIT LOG HERE ---
        asyncio.create_task(send_log(f"⚖️ <b>PVP {g_type.upper()}</b>\n<a href='tg://user?id={winner}'>{win_name}</a> won the {rule_text.lower()} rule and took a <b>{payout:,} Gold</b> pot!"))
        
        await bot.send_message(
            chat_id,
            f"👑 <b>WINNER: @{win_name}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"They take home the <b>{payout:,} Gold</b> pot!\n"
            f"<i>(5% House Tax Applied)</i>", parse_mode="HTML"
        )
    else:
        await update_player_balance(c_id, game['bet'])
        await update_player_balance(game['defender'], game['bet'])
        await bot.send_message(
            chat_id,
            f"🤝 <b>IT'S A DRAW!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Exact tie! The <b>{game['bet']:,} Gold</b> escrow has been refunded to both vaults.", parse_mode="HTML"
        )
        
    del challenge_games[c_id]

async def setup_pvp_mines(msg: types.Message, game: dict):
    bombs = random.randint(3, 7)
    game.update({
        "mines": random.sample(range(25), bombs),
        "revealed": [],
        "turn": game['challenger'], # Challenger always goes first
        "bombs_count": bombs,
        "chat_id": msg.chat.id
    })
    
    text, kb = get_pvp_mines_ui(game)
    bot_msg = await bot.send_message(game['chat_id'], text, reply_markup=kb, parse_mode="HTML")
    game['msg_id'] = bot_msg.message_id

def get_pvp_mines_ui(game: dict, exploded_idx=None):
    c_id = game['challenger']
    turn_name = game['c_name'] if game['turn'] == c_id else game['d_name']
    
    text = (
        f"💣 <b>𝗣𝗩𝗣 𝗠𝗜𝗡𝗘𝗙𝗜𝗘𝗟𝗗</b> 💣\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Pot:</b> {game['bet'] * 2:,} Gold\n"
        f"⚠️ <b>Hidden Bombs:</b> {game['bombs_count']}\n\n"
    )
    
    if exploded_idx is not None:
        text += f"💥 <b>𝗕𝗢𝗢𝗠!</b> @{turn_name} stepped on a mine!"
    else:
        text += f"👉 <b>It is @{turn_name}'s turn to step...</b>"
        
    buttons = []
    for i in range(25):
        if exploded_idx is not None:
            # Reveal Board
            if i == exploded_idx: btn_text = "💥"
            elif i in game['mines']: btn_text = "💣"
            elif i in game['revealed']: btn_text = "💎"
            else: btn_text = "❓"
        else:
            # Active Board
            btn_text = "💎" if i in game['revealed'] else "❓"
            
        buttons.append(types.InlineKeyboardButton(text=btn_text, callback_data=f"pvp_mine_hit_{c_id}_{i}"))
        
    kb = [buttons[i:i + 5] for i in range(0, 25, 5)]
    return text, types.InlineKeyboardMarkup(inline_keyboard=kb)

@dp.callback_query(F.data.startswith("pvp_mine_hit_"))
async def handle_pvp_mine_click(call: types.CallbackQuery):
    parts = call.data.split("_")
    c_id = int(parts[3])
    idx = int(parts[4])
    
    game = challenge_games.get(c_id)
    if not game or game['status'] != 'playing': return await call.answer("❌ Game expired.", show_alert=True)
    if call.from_user.id not in [game['challenger'], game['defender']]: return await call.answer("❌ Not your fight!", show_alert=True)
    if call.from_user.id != game['turn']: return await call.answer("⏳ It is NOT your turn!", show_alert=True)
    if idx in game['revealed']: return await call.answer("⚠️ Already stepped there!", show_alert=True)

    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        # 1. THEY HIT A BOMB
        if idx in game['mines']:
            winner = game['defender'] if call.from_user.id == game['challenger'] else game['challenger']
            loser = call.from_user.id
            win_name = game['d_name'] if winner == game['defender'] else game['c_name']
            loser_name = game['c_name'] if loser == game['challenger'] else game['d_name']
            
            pot = game['bet'] * 2
            tax = int(pot * 0.05)
            payout = pot - tax
            
            p_win = await get_player_fast(types.User(id=winner, is_bot=False, first_name=""))
            await update_player_balance(winner, payout, {"wins": p_win.get('wins', 0) + 1})
            
            p_lose = await get_player_fast(types.User(id=loser, is_bot=False, first_name=""))
            await handle_loss(p_lose) 
            
            # --- ADD THE AUDIT LOG HERE ---
            asyncio.create_task(send_log(f"💣 <b>PVP MINES</b>\n<a href='tg://user?id={loser}'>{loser_name}</a> stepped on a mine! <a href='tg://user?id={winner}'>{win_name}</a> survived and took a <b>{payout:,} Gold</b> pot!"))
            
            text, kb = get_pvp_mines_ui(game, exploded_idx=idx)
            text += f"\n\n👑 <b>WINNER: @{win_name} survives!</b>\nThey take home <b>{payout:,} Gold</b>! <i>(5% House Tax)</i>"
            
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            del challenge_games[c_id]
            
        # 2. THEY ARE SAFE
        else:
            game['revealed'].append(idx)
            # Swap turn
            game['turn'] = game['defender'] if call.from_user.id == game['challenger'] else game['challenger']
            game['ts'] = datetime.now().timestamp()
            
            text, kb = get_pvp_mines_ui(game)
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            
    finally:
        active_games.discard(call.from_user.id)

@dp.callback_query(F.data.startswith("pvp_cr_set_"))
async def handle_pvp_crate_setup(call: types.CallbackQuery):
    parts = call.data.split("_")
    c_id = int(parts[3])
    c_count = int(parts[4])
    
    game = challenge_games.get(c_id)
    if not game or game['status'] != 'playing': return await call.answer("❌ Game expired.", show_alert=True)
    if call.from_user.id != game['challenger']: return await call.answer("❌ Only the Challenger can set the crates!", show_alert=True)
    if game.get('crates_set'): return await call.answer("⏳ Crates already dropped.", show_alert=True)
    
    game['crates_set'] = True
    game['c_count'] = c_count
    game['picks'] = {} # Stores {user_id: crate_idx}
    
    # Randomly distribute the total pot across the chosen crates
    pot = game['bet'] * 2
    splits = [random.randint(1, 100) for _ in range(c_count)]
    total_weight = sum(splits)
    game['crate_values'] = [int(pot * (w / total_weight)) for w in splits]
    
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except: pass
    
    text, kb = get_pvp_crates_ui(game)
    bot_msg = await bot.send_message(game['chat_id'], text, reply_markup=kb, parse_mode="HTML")
    game['msg_id'] = bot_msg.message_id

def get_pvp_crates_ui(game: dict, reveal=False):
    c_id = game['challenger']
    
    text = (
        f"📦 <b>𝗣𝗩𝗣 𝗦𝗠𝗨𝗚𝗚𝗟𝗘𝗥'𝗦 𝗦𝗣𝗟𝗜𝗧</b> 📦\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Total Pot:</b> {game['bet'] * 2:,} Gold\n\n"
    )
    
    if not reveal:
        text += f"<i>Both players must pick ONE crate to loot. The Bouncer sweeps the rest.</i>\n\n"
        c_status = "✅ <i>Locked in</i>" if game['challenger'] in game.get('picks', {}) else "<i>Thinking...</i>"
        d_status = "✅ <i>Locked in</i>" if game['defender'] in game.get('picks', {}) else "<i>Thinking...</i>"
        text += f"🩸 @{game['c_name']}: {c_status}\n🎯 @{game['d_name']}: {d_status}"
    else:
        text += f"💥 <b>𝗧𝗛𝗘 𝗥𝗘𝗩𝗘𝗔𝗟</b> 💥\n"
        
    buttons = []
    for i in range(game['c_count']):
        if reveal:
            # Highlight who picked what, or show standard wood if swept
            val = game['crate_values'][i]
            if i == game['picks'].get(game['challenger']): btn_text = "🩸"
            elif i == game['picks'].get(game['defender']): btn_text = "🎯"
            elif val > (game['bet']*2) * 0.3: btn_text = "💰" # Highlight big unpicked crates
            else: btn_text = "🪵"
        else:
            btn_text = f"📦 {i+1}"
            
        buttons.append(types.InlineKeyboardButton(text=btn_text, callback_data=f"pvp_cr_hit_{c_id}_{i}"))
        
    kb = [buttons[i:i + 5] for i in range(0, len(buttons), 5)]
    return text, types.InlineKeyboardMarkup(inline_keyboard=kb)

@dp.callback_query(F.data.startswith("pvp_cr_hit_"))
async def handle_pvp_crate_click(call: types.CallbackQuery):
    parts = call.data.split("_")
    c_id = int(parts[3])
    idx = int(parts[4])
    
    game = challenge_games.get(c_id)
    if not game or game['status'] != 'playing': return await call.answer("❌ Game expired.", show_alert=True)
    
    uid = call.from_user.id
    if uid not in [game['challenger'], game['defender']]: return await call.answer("❌ Not your crates!", show_alert=True)
    if uid in game['picks']: return await call.answer("⚠️ You already picked a crate!", show_alert=True)
    
    # Prevent picking the exact same crate
    if idx in game['picks'].values(): return await call.answer("❌ Opponent already grabbed that one!", show_alert=True)

    if uid in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(uid)
    
    try:
        game['picks'][uid] = idx
        
        # If waiting on opponent, update UI
        if len(game['picks']) < 2:
            text, kb = get_pvp_crates_ui(game)
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            return
            
        # --- BOTH LOCKED IN: RESOLVE ---
        c_pick_idx = game['picks'][game['challenger']]
        d_pick_idx = game['picks'][game['defender']]
        
        c_loot = game['crate_values'][c_pick_idx]
        d_loot = game['crate_values'][d_pick_idx]
        
        # House takes EVERYTHING that wasn't picked
        house_sweep = (game['bet'] * 2) - c_loot - d_loot
        
        # Update Balances
        if c_loot > 0: await update_player_balance(game['challenger'], c_loot)
        if d_loot > 0: await update_player_balance(game['defender'], d_loot)
        
        # --- ADD THE AUDIT LOG HERE ---
        asyncio.create_task(send_log(f"📦 <b>PVP CRATES</b>\n<a href='tg://user?id={game['challenger']}'>{game['c_name']}</a> looted <b>{c_loot:,} Gold</b>, <a href='tg://user?id={game['defender']}'>{game['d_name']}</a> looted <b>{d_loot:,} Gold</b>. The House swept <b>{house_sweep:,} Gold</b>!"))
        
        text, kb = get_pvp_crates_ui(game, reveal=True)
        text += (
            f"\n\n🩸 <b>@{game['c_name']}</b> opened Crate {c_pick_idx+1}! (Found: <b>{c_loot:,} Gold</b>)\n"
            f"🎯 <b>@{game['d_name']}</b> opened Crate {d_pick_idx+1}! (Found: <b>{d_loot:,} Gold</b>)\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🏦 <b>The House swept up the remaining {house_sweep:,} Gold!</b>"
        )
        
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        del challenge_games[c_id]
        
    finally:
        active_games.discard(uid)

def get_aero_lobby_ui(game: dict, host_id: int):
    """Generates the live updating lobby UI for The Smuggler's Run."""
    text = (
        f"🚐 <b>𝗧𝗛𝗘 𝗦𝗠𝗨𝗚𝗚𝗟𝗘𝗥'𝗦 𝗥𝗨𝗡</b> 🚐\n"
        f"<i>The van is fueling up. Get in before it leaves.</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Buy-In:</b> {game['bet']:,} Gold\n\n"
        f"👥 <b>Passengers ({len(game['players'])}/5):</b>\n"
    )
    
    for pid, pname in game['players'].items():
        role = "👑 Host" if pid == host_id else "🎟️ Passenger"
        text += f"• @{pname} ({role})\n"
        
    text += f"━━━━━━━━━━━━━━━━━━\n"
    
    kb_rows = []
    if len(game['players']) < 5:
        kb_rows.append([types.InlineKeyboardButton(text=f"🎟️ Buy In ({format_suffix(game['bet'])})", callback_data=f"aero_buy_{host_id}")])
    
    kb_rows.append([
        types.InlineKeyboardButton(text="🚀 Launch Plane", callback_data=f"aero_launch_{host_id}"),
        types.InlineKeyboardButton(text="❌ Cancel Flight", callback_data=f"aero_cancel_{host_id}")
    ])
    
    return text, types.InlineKeyboardMarkup(inline_keyboard=kb_rows)


@dp.message(Command("aeroplane"))
async def cmd_aeroplane(m: types.Message):
    if m.chat.type == "private":
        return await m.answer("❌ This is a group-only game!")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit():
        return await m.answer("❌ <b>Usage:</b> `/aeroplane [amount]`", parse_mode="HTML")
        
    amount = int(args[1])
    if amount < 100:
        return await m.answer("❌ Minimum buy-in is 100 Gold.")

    if m.from_user.id in active_games: return await m.answer("⏳ You are busy...")
    if m.from_user.id in aeroplane_games: return await m.answer("❌ You already have a flight boarding!")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        if p['balance'] < amount:
            return await m.answer("❌ Insufficient vault balance to host this flight.")

        # Escrow the Host's money immediately
        await update_player_balance(m.from_user.id, -amount)

        safe_name = html.escape(m.from_user.username or m.from_user.first_name)
        
        aeroplane_games[m.from_user.id] = {
            "host_id": m.from_user.id,
            "bet": amount,
            "players": {m.from_user.id: safe_name}, # Dictionary of {user_id: username}
            "status": "lobby",
            "ts": datetime.now().timestamp()
        }

        text, kb = get_aero_lobby_ui(aeroplane_games[m.from_user.id], m.from_user.id)
        bot_msg = await m.answer(text, reply_markup=kb, parse_mode="HTML")
        
        # Save message details to edit it later during the flight
        aeroplane_games[m.from_user.id]['chat_id'] = bot_msg.chat.id
        aeroplane_games[m.from_user.id]['msg_id'] = bot_msg.message_id
        
    finally:
        active_games.discard(m.from_user.id)


@dp.callback_query(F.data.startswith("aero_buy_"))
async def handle_aero_buy(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    game = aeroplane_games.get(host_id)
    
    if not game or game['status'] != 'lobby': 
        return await call.answer("❌ Flight has already departed or expired!", show_alert=True)
        
    if call.from_user.id in game['players']:
        return await call.answer("⚠️ You are already on the plane!", show_alert=True)
        
    if len(game['players']) >= 5:
        return await call.answer("❌ The van is completely full!", show_alert=True)

    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        p = await get_player_fast(call.from_user)
        if p['balance'] < game['bet']:
            return await call.answer(f"❌ You need {game['bet']:,} Gold to buy a ticket!", show_alert=True)

        # Escrow the Passenger's money
        await update_player_balance(call.from_user.id, -game['bet'])
        
        safe_name = html.escape(call.from_user.username or call.from_user.first_name)
        game['players'][call.from_user.id] = safe_name
        game['ts'] = datetime.now().timestamp() # Reset AFK timer
        
        text, kb = get_aero_lobby_ui(game, host_id)
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await call.answer("🎟️ Ticket purchased! Seat secured.", show_alert=False)
        
    finally:
        active_games.discard(call.from_user.id)


@dp.callback_query(F.data.startswith("aero_cancel_"))
async def handle_aero_cancel(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    if call.from_user.id != host_id: 
        return await call.answer("❌ Only the Host can cancel the flight!", show_alert=True)
        
    game = aeroplane_games.get(host_id)
    if not game or game['status'] != 'lobby': return await call.answer("❌ Too late!", show_alert=True)
    
    # Refund everyone
    for pid in game['players'].keys():
        await update_player_balance(pid, game['bet'])
        
    del aeroplane_games[host_id]
    await call.message.edit_text("❌ <b>Flight Cancelled.</b> All tickets refunded.", parse_mode="HTML")


@dp.callback_query(F.data.startswith("aero_launch_"))
async def handle_aero_launch(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    if call.from_user.id != host_id: 
        return await call.answer("❌ Only the Host can start the engine!", show_alert=True)
        
    game = aeroplane_games.get(host_id)
    if not game or game['status'] != 'lobby': return await call.answer("❌ Invalid flight state.", show_alert=True)
    
    if len(game['players']) < 2:
        return await call.answer("❌ You need at least 1 other passenger to start the run!", show_alert=True)
        
    game['status'] = 'flying'
    
    await call.message.edit_text(
        f"🚐 <b>𝗧𝗛𝗘 𝗗𝗢𝗢𝗥𝗦 𝗔𝗥𝗘 𝗟𝗢𝗖𝗞𝗘𝗗.</b>\n"
        f"<i>Fasten your seatbelts... The run begins in 3 seconds.</i>",
        parse_mode="HTML"
    )
    
    asyncio.create_task(run_aeroplane_flight(host_id))

async def run_aeroplane_flight(host_id: int):
    """The core engine for the Aeroplane (Crash) game."""
    game = aeroplane_games.get(host_id)
    if not game: return

    # 1. Pre-roll the Crash Point
    # Weighted RNG: 10% Early Crash, 85% Standard, 5% Moon
    roll = random.random()
    if roll < 0.10: # Instant/Early Crash
        crash_point = random.uniform(1.0, 1.5)
    elif roll > 0.95: # Moon Run
        crash_point = random.uniform(10.0, 25.0)
    else: # Standard Run
        crash_point = random.uniform(1.5, 10.0)
    
    # 2. Flight State Setup
    game.update({
        "multiplier": 1.0,
        "crash_at": crash_point,
        "bailed_players": {}, # {user_id: multiplier}
        "active_pids": list(game['players'].keys())
    })

    # The Exponential Multiplier Curve (Approx 10-12 steps to reach 25x)
    # This grows slowly at first, then jumps.
    steps = [1.0, 1.2, 1.5, 1.9, 2.4, 3.2, 4.5, 6.5, 9.0, 13.0, 18.5, 25.0]

    for current_mult in steps:
        game['multiplier'] = current_mult
        
        # Check for Crash
        if current_mult >= game['crash_at']:
            await finish_aero_run(host_id, busted=True)
            return

        # Check for Max Cap (25x)
        if current_mult >= 25.0:
            await finish_aero_run(host_id, busted=False)
            return

        # Update UI for everyone
        text, kb = get_aero_flight_ui(game)
        try:
            await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=kb, parse_mode="HTML")
        except: pass

        await asyncio.sleep(3) # Your requested 3-second delay per edit

    # Final safety finish
    await finish_aero_run(host_id, busted=False)

def get_aero_flight_ui(game: dict, final_msg=None):
    """Generates the live flight UI with Bail Out buttons."""
    mult = game['multiplier']
    text = (
        f"🚐 <b>𝗧𝗛𝗘 𝗥𝗨𝗡 𝗜𝗦 𝗟𝗜𝗩𝗘</b> 🚐\n"
        f"📈 <b>Multiplier: {mult:.2f}x</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
    )
    
    for pid, pname in game['players'].items():
        if pid in game['bailed_players']:
            text += f"✅ @{pname} — <b>Bailed {game['bailed_players'][pid]:.2f}x</b>\n"
        elif final_msg:
            text += f"💥 @{pname} — <b>BUSTED!</b>\n"
        else:
            text += f"🩸 @{pname} — <i>Riding...</i>\n"
            
    if final_msg:
        text += f"━━━━━━━━━━━━━━━━━━\n{final_msg}"
        return text, None

    kb = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text=f"💰 BAIL OUT ({format_suffix(int(game['bet']*mult))})", callback_data=f"aero_bail_{game['host_id']}")
    ]])
    return text, kb

@dp.callback_query(F.data.startswith("aero_bail_"))
async def handle_aero_bail(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    game = aeroplane_games.get(host_id)
    
    if not game or game['status'] != 'flying': return await call.answer("❌ Too late!")
    
    uid = call.from_user.id
    if uid not in game['players'] or uid in game['bailed_players']:
        return await call.answer("❌ You are not on this flight!", show_alert=True)

    # Lock in their current multiplier
    game['bailed_players'][uid] = game['multiplier']
    await call.answer(f"✅ Bailed at {game['multiplier']:.2f}x!", show_alert=True)
    
    # If everyone has bailed, finish early
    if len(game['bailed_players']) == len(game['players']):
        # Using a small task to avoid blocking the callback
        asyncio.create_task(finish_aero_run(host_id, busted=False))

async def finish_aero_run(host_id: int, busted: bool):
    game = aeroplane_games.get(host_id)
    if not game or game['status'] == 'finished': return
    game['status'] = 'finished'

    crash_val = game['multiplier']
    if busted:
        final_msg = f"💥 <b>𝗖𝗥𝗔𝗦𝗛𝗘𝗗 𝗔𝗧 {crash_val:.2f}𝘅!</b>\n<i>The van was intercepted by the police.</i>"
    else:
        final_msg = f"💎 <b>𝗝𝗔𝗖𝗞𝗣𝗢𝗧! 𝟮𝟱.𝟬𝟬𝘅 𝗥𝗘𝗔𝗖𝗛𝗘𝗗!</b>\n<i>The smuggler has reached the safehouse.</i>"

    # Process Payouts
    for pid, p_mult in game['bailed_players'].items():
        win_amt = int(game['bet'] * p_mult)
        p_obj = await get_player_fast(types.User(id=pid, is_bot=False, first_name=""))
        await update_player_balance(pid, win_amt, {"wins": p_obj.get('wins', 0) + 1})

    # Record Losses for those who didn't bail
    for pid in game['players'].keys():
        if pid not in game['bailed_players']:
            p_obj = await get_player_fast(types.User(id=pid, is_bot=False, first_name=""))
            await handle_loss(p_obj)

    # --- ADD THE AUDIT LOG HERE ---
    if game['bailed_players']:
        asyncio.create_task(send_log(f"✈️ <b>SMUGGLER RUN</b>\nVan crashed at <b>{crash_val:.2f}x</b>. {len(game['bailed_players'])} passengers bailed safely!"))
    else:
        asyncio.create_task(send_log(f"✈️ <b>SMUGGLER RUN</b>\nVan crashed at <b>{crash_val:.2f}x</b>. TOTAL CREW WIPE."))

    text, _ = get_aero_flight_ui(game, final_msg=final_msg)
    try:
        await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=None, parse_mode="HTML")
    except: pass
    
    # Cleanup RAM
    del aeroplane_games[host_id]

def get_roulette_lobby_ui(game: dict):
    """Generates the live updating lobby UI for Russian Roulette."""
    text = (
        f"🩸 <b>𝗥𝗨𝗦𝗦𝗜𝗔𝗡 𝗥𝗢𝗨𝗟𝗘𝗧𝗧𝗘</b> 🩸\n"
        f"<i>The table is open. The gun is loaded.</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Buy-In:</b> {game['bet']:,} Gold\n"
        f"🏆 <b>Current Pot:</b> {game['bet'] * len(game['players']):,} Gold\n\n"
        f"👥 <b>Players ({len(game['players'])}/6):</b>\n"
    )
    
    for pid in game['players']:
        role = "👑 Host" if pid == game['host_id'] else "🎟️ Player"
        text += f"• @{game['names'][pid]} ({role})\n"
        
    text += f"━━━━━━━━━━━━━━━━━━\n"
    
    kb_rows = []
    if len(game['players']) < 6:
        kb_rows.append([types.InlineKeyboardButton(text=f"🎟️ Sit at Table ({format_suffix(game['bet'])})", callback_data=f"rr_join_{game['host_id']}")])
    
    kb_rows.append([
        types.InlineKeyboardButton(text="🔫 Spin the Cylinder", callback_data=f"rr_start_{game['host_id']}"),
        types.InlineKeyboardButton(text="❌ Walk Away", callback_data=f"rr_cancel_{game['host_id']}")
    ])
    
    return text, types.InlineKeyboardMarkup(inline_keyboard=kb_rows)


@dp.message(Command("roulette"))
async def cmd_roulette(m: types.Message):
    if m.chat.type == "private":
        return await m.answer("❌ This is a group-only game!")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit():
        return await m.answer("❌ <b>Usage:</b> `/roulette [amount]`", parse_mode="HTML")
        
    amount = int(args[1])
    if amount <= 0:
        return await m.answer("❌ The Syndicate doesn't play for air. Bet something.")

    if m.from_user.id in active_games: return await m.answer("⏳ You are busy...")
    if m.from_user.id in roulette_games: return await m.answer("❌ You already have a table open!")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        if p['balance'] < amount:
            return await m.answer("❌ Insufficient vault balance to host this table.")

        # Escrow the Host's money
        await update_player_balance(m.from_user.id, -amount)

        safe_name = html.escape(m.from_user.username or m.from_user.first_name)
        
        roulette_games[m.from_user.id] = {
            "host_id": m.from_user.id,
            "bet": amount,
            "players": [m.from_user.id], # Ordered list for turns
            "names": {m.from_user.id: safe_name},
            "status": "lobby",
            "ts": datetime.now().timestamp()
        }

        text, kb = get_roulette_lobby_ui(roulette_games[m.from_user.id])
        bot_msg = await m.answer(text, reply_markup=kb, parse_mode="HTML")
        
        # Save message details so the engine can edit it later
        roulette_games[m.from_user.id]['chat_id'] = bot_msg.chat.id
        roulette_games[m.from_user.id]['msg_id'] = bot_msg.message_id
        
    finally:
        active_games.discard(m.from_user.id)


@dp.callback_query(F.data.startswith("rr_join_"))
async def handle_rr_join(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    game = roulette_games.get(host_id)
    
    if not game or game['status'] != 'lobby': 
        return await call.answer("❌ Table is closed or game started!", show_alert=True)
        
    if call.from_user.id in game['players']:
        return await call.answer("⚠️ You are already sitting at the table!", show_alert=True)
        
    if len(game['players']) >= 6:
        return await call.answer("❌ The table is completely full!", show_alert=True)

    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        p = await get_player_fast(call.from_user)
        if p['balance'] < game['bet']:
            return await call.answer(f"❌ You need {game['bet']:,} Gold to sit down!", show_alert=True)

        # Escrow the Passenger's money
        await update_player_balance(call.from_user.id, -game['bet'])
        
        safe_name = html.escape(call.from_user.username or call.from_user.first_name)
        game['players'].append(call.from_user.id)
        game['names'][call.from_user.id] = safe_name
        game['ts'] = datetime.now().timestamp() # Reset AFK timer
        
        text, kb = get_roulette_lobby_ui(game)
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await call.answer("🎟️ You sat down at the table.", show_alert=False)
        
    finally:
        active_games.discard(call.from_user.id)


@dp.callback_query(F.data.startswith("rr_cancel_"))
async def handle_rr_cancel(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    if call.from_user.id != host_id: 
        return await call.answer("❌ Only the Host can close the table!", show_alert=True)
        
    game = roulette_games.get(host_id)
    if not game or game['status'] != 'lobby': return await call.answer("❌ Too late!", show_alert=True)
    
    # Refund everyone at the table
    for pid in game['players']:
        await update_player_balance(pid, game['bet'])
        
    del roulette_games[host_id]
    await call.message.edit_text("❌ <b>Table Closed.</b> All gold refunded.", parse_mode="HTML")


@dp.callback_query(F.data.startswith("rr_start_"))
async def handle_rr_start(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    if call.from_user.id != host_id: 
        return await call.answer("❌ Only the Host can start the game!", show_alert=True)
        
    game = roulette_games.get(host_id)
    if not game or game['status'] != 'lobby': return await call.answer("❌ Invalid state.", show_alert=True)
    
    if len(game['players']) < 2:
        return await call.answer("❌ You need at least 1 other player to spin the cylinder!", show_alert=True)
        
    game['status'] = 'playing'
    
    await call.message.edit_text(
        f"🩸 <b>𝗧𝗛𝗘 𝗚𝗨𝗡 𝗜𝗦 𝗟𝗢𝗔𝗗𝗘𝗗</b>\n"
        f"<i>The doors lock. Randomizing turn order...</i>",
        parse_mode="HTML"
    )
    
    asyncio.create_task(run_roulette_turn(host_id))

async def run_roulette_turn(host_id: int):
    """Manages the turn order and logic for Russian Roulette."""
    game = roulette_games.get(host_id)
    if not game or game['status'] != 'playing': return

    # 1. Check for Last Man Standing
    if len(game['players']) == 1:
        winner_id = game['players'][0]
        await finish_roulette(host_id, winner_id)
        return

    # 2. Initialize Game Variables on first run
    if 'chambers' not in game:
        game['chambers'] = 6
        random.shuffle(game['players']) # Randomize seating order!
        game['turn_idx'] = 0

    # 3. Setup Current Turn
    current_player = game['players'][game['turn_idx']]
    p_name = game['names'][current_player]

    game['expected_ts'] = datetime.now().timestamp()
    game['waiting_for'] = current_player

    # 4. Build UI
    total_pot = game['bet'] * len(game['names'])
    text = (
        f"🩸 <b>𝗥𝗨𝗦𝗦𝗜𝗔𝗡 𝗥𝗢𝗨𝗟𝗘𝗧𝗧𝗘</b> 🩸\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 <b>Survivors:</b> {len(game['players'])}\n"
        f"💰 <b>Pot:</b> {total_pot:,} Gold\n"
        f"🔫 <b>Odds of Death:</b> 1 in {game['chambers']}\n\n"
        f"👉 <b>It is @{p_name}'s turn.</b>\n"
        f"<i>You have 30 seconds to pull the trigger...</i>"
    )
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="💥 Pull Trigger", callback_data=f"rr_shoot_{host_id}")
    ]])

    try:
        await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=kb, parse_mode="HTML")
    except: pass

    # 5. Start the Execution Timer
    asyncio.create_task(rr_afk_timer(host_id, game['expected_ts'], current_player))


async def rr_afk_timer(host_id: int, expected_ts: float, player_id: int):
    """Executes a player if they refuse to pull the trigger."""
    await asyncio.sleep(30)
    game = roulette_games.get(host_id)
    
    if game and game.get('status') == 'playing' and game.get('expected_ts') == expected_ts:
        game['waiting_for'] = None # Lock out manual clicks
        p_name = game['names'][player_id]

        text = (
            f"💀 <b>𝗘𝗫𝗘𝗖𝗨𝗧𝗘𝗗!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"@{p_name} froze in fear.\n"
            f"<i>The Bouncer steps in and handles it.</i>\n\n"
            f"🩸 <b>@{p_name} is eliminated!</b>"
        )
        try:
            await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=None, parse_mode="HTML")
        except: pass

        # Eliminate them, but keep their money in the pot
        game['players'].remove(player_id)

        # Fix turn index overflow
        if game['turn_idx'] >= len(game['players']):
            game['turn_idx'] = 0

        game['chambers'] = 6 # Reload gun after a death

        await asyncio.sleep(3)
        asyncio.create_task(run_roulette_turn(host_id))


@dp.callback_query(F.data.startswith("rr_shoot_"))
async def handle_rr_shoot(call: types.CallbackQuery):
    """Handles the 1-in-X RNG of pulling the trigger."""
    host_id = int(call.data.split("_")[2])
    game = roulette_games.get(host_id)

    if not game or game['status'] != 'playing':
        return await call.answer("❌ Game over or expired.", show_alert=True)

    if call.from_user.id != game.get('waiting_for'):
        return await call.answer("❌ It is NOT your turn! Back away from the gun.", show_alert=True)

    # Lock the trigger
    game['waiting_for'] = None
    p_name = game['names'][call.from_user.id]

    # RNG: 1 in X (The shrinking cylinder)
    is_dead = random.randint(1, game['chambers']) == 1

    if is_dead:
        # 💥 THE GUN WENT OFF
        text = (
            f"💥 <b>𝗕𝗔𝗡𝗚!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"@{p_name} pulls the trigger...\n"
            f"💀 <b>They are dead.</b>"
        )
        await call.message.edit_text(text, reply_markup=None, parse_mode="HTML")

        game['players'].remove(call.from_user.id)
        if game['turn_idx'] >= len(game['players']):
            game['turn_idx'] = 0

        game['chambers'] = 6 # Reload the gun for the remaining players

        await asyncio.sleep(3.5)
        asyncio.create_task(run_roulette_turn(host_id))
        
    else:
        # 💨 EMPTY CHAMBER
        game['chambers'] -= 1
        text = (
            f"💨 <b>*𝗖𝗟𝗜𝗖𝗞*</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"@{p_name} pulls the trigger...\n"
            f"😅 <b>Empty chamber. They survive.</b>"
        )
        await call.message.edit_text(text, reply_markup=None, parse_mode="HTML")

        # Pass to the next player
        game['turn_idx'] += 1
        if game['turn_idx'] >= len(game['players']):
            game['turn_idx'] = 0

        await asyncio.sleep(2.5)
        asyncio.create_task(run_roulette_turn(host_id))


async def finish_roulette(host_id: int, winner_id: int):
    """Pays out the final survivor and cleans up."""
    game = roulette_games.get(host_id)
    if not game: return

    total_pot = game['bet'] * len(game['names'])
    tax = int(total_pot * 0.05) # 5% House Edge
    payout = total_pot - tax
    winner_name = game['names'][winner_id]

    # Pay the Survivor
    p_win = await get_player_fast(types.User(id=winner_id, is_bot=False, first_name=""))
    await update_player_balance(winner_id, payout, {"wins": p_win.get('wins', 0) + 1})

    # --- ADD THE AUDIT LOG HERE ---
    asyncio.create_task(send_log(f"🩸 <b>ROULETTE SURVIVOR</b>\n<a href='tg://user?id={winner_id}'>{winner_name}</a> survived the cylinder and swept the <b>{payout:,} Gold</b> pot!"))

    text = (
        f"🏆 <b>𝗥𝗢𝗨𝗟𝗘𝗧𝗧𝗘 𝗦𝗨𝗥𝗩𝗜𝗩𝗢𝗥</b> 🏆\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🩸 <b>@{winner_name}</b> is the last one standing!\n\n"
        f"💰 They sweep the bloody <b>{payout:,} Gold</b> pot!\n"
        f"<i>(5% House Tax Applied to the {total_pot:,} Pot)</i>"
    )

    try:
        await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=None, parse_mode="HTML")
    except: pass

    # Sweep RAM
    del roulette_games[host_id]
 
def get_poker_lobby_ui(game: dict):
    """Generates the live updating lobby UI for The Standoff."""
    text = (
        f"🃏 <b>𝗧𝗛𝗘 𝗦𝗧𝗔𝗡𝗗𝗢𝗙𝗙</b> 🃏\n"
        f"<i>A high-stakes blind table has opened.</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Buy-In:</b> {game['bet']:,} Gold\n"
        f"🏆 <b>Current Pot:</b> {game['bet'] * len(game['players']):,} Gold\n\n"
        f"👥 <b>Players ({len(game['players'])}/6):</b>\n"
    )
    
    for pid in game['players']:
        role = "👑 Host" if pid == game['host_id'] else "🎟️ Player"
        text += f"• @{game['names'][pid]} ({role})\n"
        
    text += f"━━━━━━━━━━━━━━━━━━\n"
    
    kb_rows = []
    if len(game['players']) < 6:
        kb_rows.append([types.InlineKeyboardButton(text=f"🎟️ Sit at Table ({format_suffix(game['bet'])})", callback_data=f"pk_join_{game['host_id']}")])
    
    kb_rows.append([
        types.InlineKeyboardButton(text="🃏 Deal Cards", callback_data=f"pk_start_{game['host_id']}"),
        types.InlineKeyboardButton(text="❌ Close Table", callback_data=f"pk_cancel_{game['host_id']}")
    ])
    
    return text, types.InlineKeyboardMarkup(inline_keyboard=kb_rows)


@dp.message(Command("poker"))
async def cmd_poker(m: types.Message):
    if m.chat.type == "private":
        return await m.answer("❌ This is a group-only game!")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit():
        return await m.answer("❌ <b>Usage:</b> `/poker [amount]`", parse_mode="HTML")
        
    amount = int(args[1])
    if amount <= 0: return await m.answer("❌ Invalid bet amount.")

    if m.from_user.id in active_games: return await m.answer("⏳ You are busy...")
    if m.from_user.id in poker_games: return await m.answer("❌ You already have a table open!")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        if p['balance'] < amount:
            return await m.answer("❌ Insufficient vault balance to host this table.")

        # Escrow the Host's money
        await update_player_balance(m.from_user.id, -amount)

        safe_name = html.escape(m.from_user.username or m.from_user.first_name)
        
        poker_games[m.from_user.id] = {
            "host_id": m.from_user.id,
            "bet": amount,         # The initial buy-in
            "players": [m.from_user.id], # Ordered list of active players
            "names": {m.from_user.id: safe_name},
            "status": "lobby",
            "ts": datetime.now().timestamp()
        }

        text, kb = get_poker_lobby_ui(poker_games[m.from_user.id])
        bot_msg = await m.answer(text, reply_markup=kb, parse_mode="HTML")
        
        # Save message details for the live game editor
        poker_games[m.from_user.id]['chat_id'] = bot_msg.chat.id
        poker_games[m.from_user.id]['msg_id'] = bot_msg.message_id
        
    finally:
        active_games.discard(m.from_user.id)


@dp.callback_query(F.data.startswith("pk_join_"))
async def handle_pk_join(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    game = poker_games.get(host_id)
    
    if not game or game['status'] != 'lobby': 
        return await call.answer("❌ Table is closed or game started!", show_alert=True)
        
    if call.from_user.id in game['players']:
        return await call.answer("⚠️ You are already sitting at the table!", show_alert=True)
        
    if len(game['players']) >= 6:
        return await call.answer("❌ The table is completely full!", show_alert=True)

    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        p = await get_player_fast(call.from_user)
        if p['balance'] < game['bet']:
            return await call.answer(f"❌ You need {game['bet']:,} Gold to sit down!", show_alert=True)

        # Escrow the Player's money
        await update_player_balance(call.from_user.id, -game['bet'])
        
        safe_name = html.escape(call.from_user.username or call.from_user.first_name)
        game['players'].append(call.from_user.id)
        game['names'][call.from_user.id] = safe_name
        game['ts'] = datetime.now().timestamp() # Reset AFK timer
        
        text, kb = get_poker_lobby_ui(game)
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await call.answer("🎟️ You sat down at the table.", show_alert=False)
        
    finally:
        active_games.discard(call.from_user.id)


@dp.callback_query(F.data.startswith("pk_cancel_"))
async def handle_pk_cancel(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    if call.from_user.id != host_id: 
        return await call.answer("❌ Only the Host can close the table!", show_alert=True)
        
    game = poker_games.get(host_id)
    if not game or game['status'] != 'lobby': return await call.answer("❌ Too late!", show_alert=True)
    
    # Refund everyone at the table
    for pid in game['players']:
        await update_player_balance(pid, game['bet'])
        
    del poker_games[host_id]
    await call.message.edit_text("❌ <b>Table Closed.</b> All gold refunded.", parse_mode="HTML")


@dp.callback_query(F.data.startswith("pk_start_"))
async def handle_pk_start(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    if call.from_user.id != host_id: 
        return await call.answer("❌ Only the Host can deal the cards!", show_alert=True)
        
    game = poker_games.get(host_id)
    if not game or game['status'] != 'lobby': return await call.answer("❌ Invalid state.", show_alert=True)
    
    if len(game['players']) < 2:
        return await call.answer("❌ You need at least 1 other player to deal!", show_alert=True)
        
    game['status'] = 'playing'
    
    await call.message.edit_text(
        f"🃏 <b>𝗧𝗛𝗘 𝗗𝗢𝗢𝗥𝗦 𝗔𝗥𝗘 𝗟𝗢𝗖𝗞𝗘𝗗</b>\n"
        f"<i>The Dealer shuffles the deck...</i>",
        parse_mode="HTML"
    )
    
    # START THE ENGINE
    asyncio.create_task(run_poker_turn(host_id))

async def run_poker_turn(host_id: int):
    """Manages the turn order and betting for The Standoff."""
    game = poker_games.get(host_id)
    if not game or game['status'] != 'playing': return

    # 1. Check for Last Man Standing (Everyone else folded)
    if len(game['players']) == 1:
        await execute_poker_showdown(host_id, default_win=True)
        return

    # 2. Initialize Game Variables on first run
    if 'cards' not in game:
        # Deal the cards
        deck = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"] * 4
        random.shuffle(deck)
        
        game['cards'] = {pid: deck.pop() for pid in game['players']}
        game['player_bets'] = {pid: game['bet'] for pid in game['players']}
        game['current_bet'] = game['bet']
        game['pot'] = game['bet'] * len(game['players'])
        
        game['turn_idx'] = 0
        game['round'] = 1
        game['showdown_called'] = False
        random.shuffle(game['players']) # Randomize turn order

    # 3. Setup Current Turn
    current_player = game['players'][game['turn_idx']]
    
    # 4. Check if we circled the table and everyone matched a Showdown call
    if game['showdown_called'] and game['player_bets'][current_player] == game['current_bet']:
        await execute_poker_showdown(host_id, default_win=False)
        return

    p_name = game['names'][current_player]
    game['expected_ts'] = datetime.now().timestamp()
    game['waiting_for'] = current_player

    # Calculate exactly what this player owes to stay in the game
    to_pay_match = game['current_bet'] - game['player_bets'][current_player]
    new_double_bet = game['current_bet'] * 2
    to_pay_double = new_double_bet - game['player_bets'][current_player]

    # 5. Build UI
    text = (
        f"🃏 <b>𝗧𝗛𝗘 𝗦𝗧𝗔𝗡𝗗𝗢𝗙𝗙</b> (Round {game['round']})\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 <b>Survivors:</b> {len(game['players'])}\n"
        f"💰 <b>Current Bet:</b> {game['current_bet']:,} Gold\n"
        f"🏆 <b>Total Pot:</b> {game['pot']:,} Gold\n\n"
        f"👉 <b>It is @{p_name}'s turn.</b>\n"
        f"<i>You have 30 seconds to act...</i>"
    )
    
    # If a showdown was called, they can only Match or Fold. No more doubling.
    kb_rows = [[types.InlineKeyboardButton(text="👁️ Peek at My Card", callback_data=f"pk_peek_{host_id}")]]
    
    if game['showdown_called']:
        text += f"\n\n🚨 <b>A SHOWDOWN WAS CALLED!</b>\n<i>Match the bet or Fold.</i>"
        btn_match = f"💵 Match (Pay {format_suffix(to_pay_match)})" if to_pay_match > 0 else "💵 Check (Free)"
        kb_rows.append([
            types.InlineKeyboardButton(text=btn_match, callback_data=f"pk_act_{host_id}_match"),
            types.InlineKeyboardButton(text="🏳️ Fold", callback_data=f"pk_act_{host_id}_fold")
        ])
    else:
        btn_match = f"💵 Match (Pay {format_suffix(to_pay_match)})" if to_pay_match > 0 else "💵 Check"
        kb_rows.append([
            types.InlineKeyboardButton(text=btn_match, callback_data=f"pk_act_{host_id}_match"),
            types.InlineKeyboardButton(text=f"🔥 Double (Pay {format_suffix(to_pay_double)})", callback_data=f"pk_act_{host_id}_double"),
            types.InlineKeyboardButton(text="🏳️ Fold", callback_data=f"pk_act_{host_id}_fold")
        ])
        
        # Unlock Showdown option on Round 3+
        if game['round'] >= 3:
            btn_show = f"🚨 Force Showdown (Pay {format_suffix(to_pay_match)})" if to_pay_match > 0 else "🚨 Force Showdown"
            kb_rows.append([types.InlineKeyboardButton(text=btn_show, callback_data=f"pk_act_{host_id}_showdown")])

    kb = types.InlineKeyboardMarkup(inline_keyboard=kb_rows)

    try:
        await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=kb, parse_mode="HTML")
    except: pass

    # Start the Auto-Fold Timer
    asyncio.create_task(pk_afk_timer(host_id, game['expected_ts'], current_player))


async def pk_afk_timer(host_id: int, expected_ts: float, player_id: int):
    """Folds a player if they go AFK for 30 seconds."""
    await asyncio.sleep(30)
    game = poker_games.get(host_id)
    
    if game and game.get('status') == 'playing' and game.get('expected_ts') == expected_ts:
        game['waiting_for'] = None 
        p_name = game['names'][player_id]

        text = (
            f"⏳ <b>𝗧𝗜𝗠𝗘'𝗦 𝗨𝗣!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"@{p_name} froze under pressure.\n"
            f"<i>The Bouncer forces them to fold.</i>"
        )
        try:
            await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=None, parse_mode="HTML")
        except: pass

        game['players'].remove(player_id)
        if game['turn_idx'] >= len(game['players']):
            game['turn_idx'] = 0
            game['round'] += 1

        await asyncio.sleep(2.5)
        asyncio.create_task(run_poker_turn(host_id))


@dp.callback_query(F.data.startswith("pk_peek_"))
async def handle_pk_peek(call: types.CallbackQuery):
    """The brilliant native Telegram popup."""
    host_id = int(call.data.split("_")[2])
    game = poker_games.get(host_id)
    
    if not game or game['status'] != 'playing': return await call.answer("❌ Game over.", show_alert=True)
    if call.from_user.id not in game['players']: return await call.answer("❌ You are not in this hand!", show_alert=True)
    
    my_card = game['cards'][call.from_user.id]
    await call.answer(f"🤫 Your card is: [ {my_card} ]\n\nDon't tell anyone.", show_alert=True)


@dp.callback_query(F.data.startswith("pk_act_"))
async def handle_pk_action(call: types.CallbackQuery):
    parts = call.data.split("_")
    host_id = int(parts[2])
    action = parts[3]
    
    game = poker_games.get(host_id)
    if not game or game['status'] != 'playing': return await call.answer("❌ Game over.", show_alert=True)
    if call.from_user.id != game.get('waiting_for'): return await call.answer("❌ It is NOT your turn!", show_alert=True)
    
    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        uid = call.from_user.id
        p_name = game['names'][uid]
        p = await get_player_fast(call.from_user)
        
        # Calculate Costs
        to_pay_match = game['current_bet'] - game['player_bets'][uid]
        new_double_bet = game['current_bet'] * 2
        to_pay_double = new_double_bet - game['player_bets'][uid]
        
        cost = 0
        if action in ["match", "showdown"]: cost = to_pay_match
        elif action == "double": cost = to_pay_double
        
        # Vault Check
        if action != "fold" and p['balance'] < cost:
            return await call.answer(f"❌ You need {cost:,} Gold in your vault to do that!", show_alert=True)
            
        game['waiting_for'] = None # Lock out double clicks
        
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except: pass

        if action == "fold":
            game['players'].remove(uid)
            if game['turn_idx'] >= len(game['players']):
                game['turn_idx'] = 0
                game['round'] += 1
            await call.message.edit_text(f"🏳️ <b>@{p_name} folded!</b>", parse_mode="HTML")
            
        else:
            # They paid money
            if cost > 0:
                await update_player_balance(uid, -cost)
                game['pot'] += cost
                
            if action == "match":
                game['player_bets'][uid] = game['current_bet']
                game['turn_idx'] += 1
                await call.message.edit_text(f"💵 <b>@{p_name} matches the bet.</b>", parse_mode="HTML")
                
            elif action == "double":
                game['current_bet'] = new_double_bet
                game['player_bets'][uid] = game['current_bet']
                game['turn_idx'] += 1
                await call.message.edit_text(f"🔥 <b>@{p_name} DOUBLES THE STAKES!</b>\nThe bet is now {game['current_bet']:,} Gold.", parse_mode="HTML")
                
            elif action == "showdown":
                game['player_bets'][uid] = game['current_bet']
                game['showdown_called'] = True
                game['turn_idx'] += 1
                await call.message.edit_text(f"🚨 <b>@{p_name} CALLS A SHOWDOWN!</b>\n<i>Everyone must match or fold.</i>", parse_mode="HTML")

        # Handle wrap-around
        if game['turn_idx'] >= len(game['players']):
            game['turn_idx'] = 0
            game['round'] += 1

        await asyncio.sleep(2.0)
        asyncio.create_task(run_poker_turn(host_id))
        
    finally:
        active_games.discard(call.from_user.id)


async def execute_poker_showdown(host_id: int, default_win: bool = False):
    """Flips the cards, calculates ties, and pays out the massive pot."""
    game = poker_games.get(host_id)
    if not game: return
    game['status'] = 'finished'

    pot = game['pot']
    tax = int(pot * 0.05)
    net_pot = pot - tax

    if default_win:
        # Everyone else folded
        winner_id = game['players'][0]
        w_name = game['names'][winner_id]
        
        p_win = await get_player_fast(types.User(id=winner_id, is_bot=False, first_name=""))
        await update_player_balance(winner_id, net_pot, {"wins": p_win.get('wins', 0) + 1})
        
        text = (
            f"👑 <b>𝗧𝗔𝗕𝗟𝗘 𝗦𝗪𝗘𝗘𝗣!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Everyone else folded under the pressure.\n\n"
            f"🩸 <b>@{w_name}</b> takes the <b>{net_pot:,} Gold</b> pot without even showing their card!\n"
            f"<i>(5% House Tax Applied)</i>"
        )
    else:
        # Actual Showdown
        # Standardize ranks using the existing CARDS list from bot.py
        ranks = {card: CARDS.index(card) for card in CARDS}
        
        # Find the highest rank
        max_rank = -1
        for pid in game['players']:
            rank = ranks[game['cards'][pid]]
            if rank > max_rank: max_rank = rank
            
        # Find all players who have that max rank (Handling ties)
        winners = [pid for pid in game['players'] if ranks[game['cards'][pid]] == max_rank]
        split_pot = net_pot // len(winners)
        
        text = f"🚨 <b>𝗧𝗛𝗘 𝗦𝗛𝗢𝗪𝗗𝗢𝗪𝗡</b> 🚨\n━━━━━━━━━━━━━━━━━━\n"
        
        for pid in game['players']:
            status = "👑" if pid in winners else "💀"
            text += f"{status} @{game['names'][pid]} flips: <b>[ {game['cards'][pid]} ]</b>\n"
            if pid not in winners:
                p_lose = await get_player_fast(types.User(id=pid, is_bot=False, first_name=""))
                await handle_loss(p_lose)
                
        text += f"━━━━━━━━━━━━━━━━━━\n"
        
        if len(winners) == 1:
            w_name = game['names'][winners[0]]
            p_win = await get_player_fast(types.User(id=winners[0], is_bot=False, first_name=""))
            await update_player_balance(winners[0], split_pot, {"wins": p_win.get('wins', 0) + 1})
            text += f"👑 <b>WINNER: @{w_name}!</b>\nThey sweep the <b>{split_pot:,} Gold</b> pot!"
            # --- ADD LOG FOR SOLO SHOWDOWN WIN ---
            asyncio.create_task(send_log(f"🃏 <b>POKER WIN</b>\n<a href='tg://user?id={winners[0]}'>{w_name}</a> swept a <b>{split_pot:,} Gold</b> pot at the showdown!"))
        else:
            w_names = ", ".join([f"@{game['names'][pid]}" for pid in winners])
            for w_id in winners:
                p_win = await get_player_fast(types.User(id=w_id, is_bot=False, first_name=""))
                await update_player_balance(w_id, split_pot, {"wins": p_win.get('wins', 0) + 1})
            text += f"🤝 <b>SPLIT POT!</b>\n{w_names} tied!\nThey each take <b>{split_pot:,} Gold</b>."
            # --- ADD LOG FOR SPLIT SHOWDOWN WIN ---
            asyncio.create_task(send_log(f"🃏 <b>POKER SPLIT</b>\nMultiple players tied and split a <b>{net_pot:,} Gold</b> pot!"))

    try:
        await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=None, parse_mode="HTML")
    except: pass

    # Cleanup RAM
    del poker_games[host_id]

def get_heist_lobby_ui(game: dict):
    """Generates the live updating lobby UI for The Heist."""
    text = (
        f"🏦 <b>𝗧𝗛𝗘 𝗦𝗬𝗡𝗗𝗜𝗖𝗔𝗧𝗘 𝗛𝗘𝗜𝗦𝗧</b> 🏦\n"
        f"<i>The getaway van is idling outside...</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Buy-In:</b> {game['bet']:,} Gold\n"
        f"🏆 <b>Initial Vault:</b> {game['bet'] * len(game['players']):,} Gold\n\n"
        f"👥 <b>The Crew ({len(game['players'])}/6):</b>\n"
    )
    
    for pid in game['players']:
        role = "👑 Mastermind" if pid == game['host_id'] else "🥷 Accomplice"
        text += f"• @{game['names'][pid]} ({role})\n"
        
    text += f"━━━━━━━━━━━━━━━━━━\n"
    
    kb_rows = []
    if len(game['players']) < 6:
        kb_rows.append([types.InlineKeyboardButton(text=f"🎒 Join the Crew ({format_suffix(game['bet'])})", callback_data=f"he_join_{game['host_id']}")])
    
    kb_rows.append([
        types.InlineKeyboardButton(text="🚐 Start the Heist", callback_data=f"he_start_{game['host_id']}"),
        types.InlineKeyboardButton(text="❌ Abort Mission", callback_data=f"he_cancel_{game['host_id']}")
    ])
    
    return text, types.InlineKeyboardMarkup(inline_keyboard=kb_rows)


@dp.message(Command("heist"))
async def cmd_heist(m: types.Message):
    if m.chat.type == "private":
        return await m.answer("❌ This is a group-only operation!")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit():
        return await m.answer("❌ <b>Usage:</b> `/heist [amount]`", parse_mode="HTML")
        
    amount = int(args[1])
    if amount < 100: return await m.answer("❌ Minimum buy-in is 100 Gold.")

    if m.from_user.id in active_games: return await m.answer("⏳ You are busy...")
    if m.from_user.id in heist_games: return await m.answer("❌ You are already planning a heist!")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        if p['balance'] < amount:
            return await m.answer("❌ Insufficient vault balance to fund this operation.")

        # Escrow the Host's money
        await update_player_balance(m.from_user.id, -amount)

        safe_name = html.escape(m.from_user.username or m.from_user.first_name)
        
        heist_games[m.from_user.id] = {
            "host_id": m.from_user.id,
            "bet": amount,
            "players": [m.from_user.id],
            "names": {m.from_user.id: safe_name},
            "status": "lobby",
            "ts": datetime.now().timestamp()
        }

        text, kb = get_heist_lobby_ui(heist_games[m.from_user.id])
        bot_msg = await m.answer(text, reply_markup=kb, parse_mode="HTML")
        
        # Save message details for the live game editor
        heist_games[m.from_user.id]['chat_id'] = bot_msg.chat.id
        heist_games[m.from_user.id]['msg_id'] = bot_msg.message_id
        
    finally:
        active_games.discard(m.from_user.id)


@dp.callback_query(F.data.startswith("he_join_"))
async def handle_he_join(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    game = heist_games.get(host_id)
    
    if not game or game['status'] != 'lobby': 
        return await call.answer("❌ The van already left or the mission was aborted!", show_alert=True)
        
    if call.from_user.id in game['players']:
        return await call.answer("⚠️ You are already in the crew!", show_alert=True)
        
    if len(game['players']) >= 6:
        return await call.answer("❌ The crew is at max capacity!", show_alert=True)

    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        p = await get_player_fast(call.from_user)
        if p['balance'] < game['bet']:
            return await call.answer(f"❌ You need {game['bet']:,} Gold to join the crew!", show_alert=True)

        # Escrow the Player's money
        await update_player_balance(call.from_user.id, -game['bet'])
        
        safe_name = html.escape(call.from_user.username or call.from_user.first_name)
        game['players'].append(call.from_user.id)
        game['names'][call.from_user.id] = safe_name
        game['ts'] = datetime.now().timestamp() # Reset AFK timer
        
        text, kb = get_heist_lobby_ui(game)
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await call.answer("🎒 You joined the crew.", show_alert=False)
        
    finally:
        active_games.discard(call.from_user.id)


@dp.callback_query(F.data.startswith("he_cancel_"))
async def handle_he_cancel(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    if call.from_user.id != host_id: 
        return await call.answer("❌ Only the Mastermind can abort the mission!", show_alert=True)
        
    game = heist_games.get(host_id)
    if not game or game['status'] != 'lobby': return await call.answer("❌ Too late!", show_alert=True)
    
    # Refund everyone in the crew
    for pid in game['players']:
        await update_player_balance(pid, game['bet'])
        
    del heist_games[host_id]
    await call.message.edit_text("❌ <b>Mission Aborted.</b> All gold refunded.", parse_mode="HTML")


@dp.callback_query(F.data.startswith("he_start_"))
async def handle_he_start(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    if call.from_user.id != host_id: 
        return await call.answer("❌ Only the Mastermind can start the heist!", show_alert=True)
        
    game = heist_games.get(host_id)
    if not game or game['status'] != 'lobby': return await call.answer("❌ Invalid state.", show_alert=True)
    
    if len(game['players']) < 2:
        return await call.answer("❌ You need at least 1 accomplice to pull off this job!", show_alert=True)
        
    game['status'] = 'playing'
    
    await call.message.edit_text(
        f"🏦 <b>𝗧𝗛𝗘 𝗗𝗢𝗢𝗥𝗦 𝗔𝗥𝗘 𝗕𝗥𝗘𝗔𝗖𝗛𝗘𝗗</b>\n"
        f"<i>The crew steps into the bank. No turning back now...</i>",
        parse_mode="HTML"
    )
    
    # START THE ENGINE
    asyncio.create_task(run_heist_stage(host_id))

HEIST_STAGES = {
    1: {"name": "The Lobby", "mult": 1.2, "cops": 0.10, "desc": "The lobby is clear. Cameras disabled."},
    2: {"name": "The Security Grid", "mult": 2.0, "cops": 0.25, "desc": "Dodging the laser grid. Sweating a little."},
    3: {"name": "The Vault Door", "mult": 4.0, "cops": 0.45, "desc": "Planting the thermite. Sirens wailing in the distance."},
    4: {"name": "The Inner Safe", "mult": 8.0, "cops": 0.65, "desc": "Bagging the gold. The police have surrounded the building."},
    5: {"name": "The Police Standoff", "mult": 15.0, "cops": 0.85, "desc": "Shooting your way out through the lobby. Pure chaos."},
    6: {"name": "The Rooftop Getaway", "mult": 30.0, "cops": 0.95, "desc": "Sprinting for the chopper under heavy sniper fire."}
}

async def run_heist_stage(host_id: int):
    """Manages the UI and flow for the current stage of the bank."""
    game = heist_games.get(host_id)
    if not game or game['status'] != 'playing': return

    # 1. First Run Setup
    if 'stage' not in game:
        game.update({
            "stage": 1,
            "active": list(game['players']), # List of IDs still in the bank
            "runners": {}, # {id: {"name": name, "mult": mult}}
            "busted": [],  # List of IDs caught by cops
            "dead_drop": 0,
            "decisions": {} # {id: "run" or "push"}
        })

    # 2. Check Game Over Conditions
    if not game['active'] or game['stage'] > 6:
        await finish_heist(host_id)
        return

    stage_data = HEIST_STAGES[game['stage']]
    game['expected_ts'] = datetime.now().timestamp()
    game['decisions'] = {} # Clear choices for the new room

    # 3. Build the UI
    text = (
        f"🏦 <b>𝗦𝗧𝗔𝗚𝗘 {game['stage']}: {stage_data['name'].upper()}</b> 🏦\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Current Cut:</b> {int(game['bet'] * stage_data['mult']):,} Gold ({stage_data['mult']}x)\n"
        f"🚨 <b>Risk of Cops:</b> {int(stage_data['cops'] * 100)}%\n"
        f"🩸 <b>Dead Drop Pot:</b> {game['dead_drop']:,} Gold\n\n"
        f"<i>{stage_data['desc']}</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⏳ <b>You have 15 seconds to decide:</b>\n"
    )

    for pid in game['active']:
        text += f"• @{game['names'][pid]}: <i>Thinking...</i>\n"

    kb = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text=f"🏃 Run (Take {format_suffix(int(game['bet'] * stage_data['mult']))})", callback_data=f"he_act_{host_id}_run"),
        types.InlineKeyboardButton(text="🧨 Push Deeper", callback_data=f"he_act_{host_id}_push")
    ]])

    try:
        await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=kb, parse_mode="HTML")
    except: pass

    # 4. Start the 15s Timer
    asyncio.create_task(he_afk_timer(host_id, game['expected_ts']))


@dp.callback_query(F.data.startswith("he_act_"))
async def handle_heist_action(call: types.CallbackQuery):
    """Catches Run or Push clicks from active players."""
    parts = call.data.split("_")
    host_id = int(parts[2])
    action = parts[3]

    game = heist_games.get(host_id)
    if not game or game['status'] != 'playing': return await call.answer("❌ Too late!", show_alert=True)
    if call.from_user.id not in game['active']: return await call.answer("❌ You are not active in this stage!", show_alert=True)
    if call.from_user.id in game['decisions']: return await call.answer("⏳ Decision locked in. Waiting for crew...", show_alert=True)

    game['decisions'][call.from_user.id] = action
    await call.answer(f"✅ You chose to {action.upper()}!", show_alert=True)

    # If everyone has decided, process the room immediately
    if len(game['decisions']) == len(game['active']):
        await resolve_heist_stage(host_id, game['expected_ts'])


async def he_afk_timer(host_id: int, expected_ts: float):
    """Executes anyone who didn't click a button in 15 seconds."""
    await asyncio.sleep(15)
    game = heist_games.get(host_id)
    
    if game and game.get('status') == 'playing' and game.get('expected_ts') == expected_ts:
        # Check who didn't make a choice
        afk_players = [pid for pid in game['active'] if pid not in game['decisions']]
        
        for pid in afk_players:
            # They are automatically BUSTED for freezing up
            game['decisions'][pid] = "afk"

        await resolve_heist_stage(host_id, expected_ts)


async def resolve_heist_stage(host_id: int, expected_ts: float):
    """Calculates who ran, who pushed, and who got caught by the cops."""
    game = heist_games.get(host_id)
    if not game or game.get('expected_ts') != expected_ts: return

    game['expected_ts'] = 0 # Lock out further processing
    stage_data = HEIST_STAGES[game['stage']]
    
    survivors = []
    round_logs = []

    for pid in game['active']:
        choice = game['decisions'].get(pid, "afk")
        pname = game['names'][pid]

        if choice == "run":
            game['runners'][pid] = {"name": pname, "mult": stage_data['mult']}
            round_logs.append(f"🏃 <b>@{pname}</b> escaped with the {stage_data['mult']}x cut!")
            
        elif choice == "afk":
            game['busted'].append(pid)
            game['dead_drop'] += game['bet']
            round_logs.append(f"💀 <b>@{pname}</b> froze in fear and was arrested!")
            
        elif choice == "push":
            # RNG Roll for the Cops
            if random.random() < stage_data['cops']:
                game['busted'].append(pid)
                game['dead_drop'] += game['bet']
                round_logs.append(f"🚨 <b>@{pname}</b> pushed too deep and got BUSTED!")
            else:
                survivors.append(pid)
                round_logs.append(f"🧨 <b>@{pname}</b> pushed deeper and survived!")

    # Update active players for the next room
    game['active'] = survivors
    
    # Show the results of the room
    text = f"💥 <b>𝗦𝗧𝗔𝗚𝗘 {game['stage']} 𝗥𝗘𝗦𝗢𝗟𝗨𝗧𝗜𝗢𝗡</b> 💥\n━━━━━━━━━━━━━━━━━━\n"
    for log in round_logs:
        text += f"{log}\n"
        
    try:
        await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=None, parse_mode="HTML")
    except: pass

    game['stage'] += 1
    await asyncio.sleep(4.0) # Let them read the logs before the next room or ending
    
    if not game['active'] or game['stage'] > 6:
        await finish_heist(host_id)
    else:
        asyncio.create_task(run_heist_stage(host_id))


async def finish_heist(host_id: int):
    """Pays out the runners, splits the Dead Drop, and calculates house tax."""
    game = heist_games.get(host_id)
    if not game or game['status'] == 'finished': return
    game['status'] = 'finished'

    # If anyone survived Stage 6, they are automatically runners at 30x
    for pid in game['active']:
        game['runners'][pid] = {"name": game['names'][pid], "mult": 30.0}

    text = f"🏦 <b>𝗛𝗘𝗜𝗦𝗧 𝗖𝗢𝗡𝗖𝗟𝗨𝗗𝗘𝗗</b> 🏦\n━━━━━━━━━━━━━━━━━━\n"
    
    if not game['runners']:
        text += f"💀 <b>TOTAL CREW WIPE.</b>\nEveryone got greedy. The cops recovered the entire <b>{game['dead_drop']:,} Gold</b> Dead Drop!"
    else:
        split_bonus = game['dead_drop'] // len(game['runners'])
        text += f"🎒 <b>THE DEAD DROP:</b> {game['dead_drop']:,} Gold\n<i>(Split evenly: +{split_bonus:,} per runner)</i>\n\n"
        
        for pid, data in game['runners'].items():
            base_win = int(game['bet'] * data['mult'])
            gross_win = base_win + split_bonus
            tax = int(gross_win * 0.05) # 5% House Edge
            net_win = gross_win - tax
            
            p_win = await get_player_fast(types.User(id=pid, is_bot=False, first_name=""))
            await update_player_balance(pid, net_win, {"wins": p_win.get('wins', 0) + 1})
            
            text += f"🏃 <b>@{data['name']}</b> (Ran at {data['mult']}x)\n"
            text += f"└ Took <b>{net_win:,} Gold</b> total!\n"

    # Log the losers silently
    for pid in game['busted']:
        p_lose = await get_player_fast(types.User(id=pid, is_bot=False, first_name=""))
        await handle_loss(p_lose)

    # --- ADD THE AUDIT LOG HERE ---
    if not game['runners']:
        asyncio.create_task(send_log(f"🏦 <b>HEIST FAILED</b>\nTotal crew wipe. The cops recovered a <b>{game['dead_drop']:,} Gold</b> Dead Drop!"))
    else:
        asyncio.create_task(send_log(f"🏦 <b>HEIST SUCCESS</b>\n{len(game['runners'])} runners escaped, splitting a <b>{game['dead_drop']:,} Gold</b> Dead Drop!"))

    text += f"━━━━━━━━━━━━━━━━━━\n<i>(5% House Tax applied to all secure lockboxes)</i>"

    try:
        await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=None, parse_mode="HTML")
    except: pass

    # Clean RAM
    del heist_games[host_id]

def get_racing_lobby_ui(game: dict):
    """Generates the live Bookie betting slip for the race."""
    theme = RACING_THEMES[game['track']]
    total_pot = game['bet'] * len(game['bets'])
    
    text = (
        f"{theme['icon']} <b>{theme['name'].upper()}</b> {theme['icon']}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎟️ <b>Ticket Price:</b> {game['bet']:,} Gold\n"
        f"🏆 <b>Total Pot:</b> {total_pot:,} Gold\n\n"
        f"📊 <b>THE BETTING SLIP:</b>\n"
    )
    
    # Tally up the bets for each racer
    for i, racer in enumerate(theme['racers']):
        backers = [data['name'] for uid, data in game['bets'].items() if data['racer_idx'] == i]
        if backers:
            text += f"• {racer}: <b>{len(backers)} Bets</b> <i>(@{', @'.join(backers)})</i>\n"
        else:
            text += f"• {racer}: <i>0 Bets</i>\n"
            
    text += f"━━━━━━━━━━━━━━━━━━\n<i>Pick your racer before the starting gun...</i>"
    
    # Build the keyboard
    kb_rows = []
    # Two racers per row for clean UI
    for i in range(0, 4, 2):
        r1 = theme['racers'][i].split()[0] # Get just the emoji/color
        r2 = theme['racers'][i+1].split()[0]
        kb_rows.append([
            types.InlineKeyboardButton(text=f"Bet {r1}", callback_data=f"rc_bet_{game['host_id']}_{i}"),
            types.InlineKeyboardButton(text=f"Bet {r2}", callback_data=f"rc_bet_{game['host_id']}_{i+1}")
        ])
    # 5th racer gets their own row
    r5 = theme['racers'][4].split()[0]
    kb_rows.append([types.InlineKeyboardButton(text=f"Bet {r5}", callback_data=f"rc_bet_{game['host_id']}_4")])
    
    kb_rows.append([
        types.InlineKeyboardButton(text="🚦 Start Race", callback_data=f"rc_start_{game['host_id']}"),
        types.InlineKeyboardButton(text="❌ Cancel Event", callback_data=f"rc_cancel_{game['host_id']}")
    ])
    
    return text, types.InlineKeyboardMarkup(inline_keyboard=kb_rows)


@dp.message(Command("racing"))
async def cmd_racing(m: types.Message):
    if m.chat.type == "private":
        return await m.answer("❌ The races are held in the main group only!")
        
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit():
        return await m.answer("❌ <b>Usage:</b> `/racing [amount]`", parse_mode="HTML")
        
    amount = int(args[1])
    if amount < 100: return await m.answer("❌ Minimum bet is 100 Gold.")

    if m.from_user.id in active_games: return await m.answer("⏳ You are busy...")
    if m.from_user.id in racing_games: return await m.answer("❌ You are already hosting a race!")

    active_games.add(m.from_user.id)
    try:
        p = await get_player_fast(m.from_user)
        if p['balance'] < amount:
            return await m.answer("❌ Insufficient vault balance to host this event.")

        # The Host pays immediately. We give them a "free bet" token for when the track opens.
        await update_player_balance(m.from_user.id, -amount)

        racing_games[m.from_user.id] = {
            "host_id": m.from_user.id,
            "bet": amount,
            "status": "selecting_track",
            "track": None,
            "bets": {}, # {user_id: {"name": safe_name, "racer_idx": 0}}
            "host_token": True, # True = Host still needs to pick their racer
            "ts": datetime.now().timestamp()
        }

        # Step 1: Ask Host to pick a track
        text = (
            f"🏁 <b>𝗧𝗛𝗘 𝗦𝗬𝗡𝗗𝗜𝗖𝗔𝗧𝗘 𝗥𝗔𝗖𝗘𝗪𝗔𝗬𝗦</b> 🏁\n"
            f"<i>Select the track for this event.</i>"
        )
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🏎️ Cars", callback_data=f"rc_track_{m.from_user.id}_cars"),
             types.InlineKeyboardButton(text="🏍️ Bikes", callback_data=f"rc_track_{m.from_user.id}_bikes")],
            [types.InlineKeyboardButton(text="🐎 Horses", callback_data=f"rc_track_{m.from_user.id}_horses")],
            [types.InlineKeyboardButton(text="❌ Cancel", callback_data=f"rc_cancel_{m.from_user.id}")]
        ])
        
        bot_msg = await m.answer(text, reply_markup=kb, parse_mode="HTML")
        racing_games[m.from_user.id]['chat_id'] = bot_msg.chat.id
        racing_games[m.from_user.id]['msg_id'] = bot_msg.message_id
        
    finally:
        active_games.discard(m.from_user.id)


@dp.callback_query(F.data.startswith("rc_track_"))
async def handle_rc_track(call: types.CallbackQuery):
    parts = call.data.split("_")
    host_id = int(parts[2])
    track = parts[3]
    
    if call.from_user.id != host_id:
        return await call.answer("❌ Only the Host can select the track!", show_alert=True)
        
    game = racing_games.get(host_id)
    if not game or game['status'] != 'selecting_track': 
        return await call.answer("❌ Invalid state.", show_alert=True)
        
    game['track'] = track
    game['status'] = 'betting'
    game['ts'] = datetime.now().timestamp()
    
    text, kb = get_racing_lobby_ui(game)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer(f"🏁 {track.upper()} selected. The Bookie is open.")


@dp.callback_query(F.data.startswith("rc_bet_"))
async def handle_rc_bet(call: types.CallbackQuery):
    parts = call.data.split("_")
    host_id = int(parts[2])
    racer_idx = int(parts[3])
    
    game = racing_games.get(host_id)
    if not game or game['status'] != 'betting': 
        return await call.answer("❌ The betting window is closed!", show_alert=True)
        
    if call.from_user.id in game['bets']:
        return await call.answer("⚠️ You already placed your bet!", show_alert=True)

    if call.from_user.id in active_games: return await call.answer("⏳ Processing...", show_alert=True)
    active_games.add(call.from_user.id)
    
    try:
        # Check if this is the Host using their prepaid token
        is_host_token = (call.from_user.id == host_id and game['host_token'])
        
        if not is_host_token:
            p = await get_player_fast(call.from_user)
            if p['balance'] < game['bet']:
                return await call.answer(f"❌ You need {game['bet']:,} Gold to place a bet!", show_alert=True)
            # Charge the player
            await update_player_balance(call.from_user.id, -game['bet'])
        else:
            game['host_token'] = False # Consume the token
            
        safe_name = html.escape(call.from_user.username or call.from_user.first_name)
        game['bets'][call.from_user.id] = {"name": safe_name, "racer_idx": racer_idx}
        game['ts'] = datetime.now().timestamp() 
        
        text, kb = get_racing_lobby_ui(game)
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await call.answer("🎫 Bet placed!", show_alert=False)
        
    finally:
        active_games.discard(call.from_user.id)


@dp.callback_query(F.data.startswith("rc_cancel_"))
async def handle_rc_cancel(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    if call.from_user.id != host_id: 
        return await call.answer("❌ Only the Host can cancel the event!", show_alert=True)
        
    game = racing_games.get(host_id)
    if not game or game['status'] in ['racing', 'finished']: 
        return await call.answer("❌ Too late!", show_alert=True)
    
    # Refund everyone who placed a bet
    for uid in game['bets']:
        await update_player_balance(uid, game['bet'])
        
    # If the Host hasn't placed their bet yet, their token is still floating. Refund them.
    if game['host_token']:
        await update_player_balance(host_id, game['bet'])
        
    del racing_games[host_id]
    await call.message.edit_text("❌ <b>Event Cancelled.</b> All bets refunded.", parse_mode="HTML")


@dp.callback_query(F.data.startswith("rc_start_"))
async def handle_rc_start(call: types.CallbackQuery):
    host_id = int(call.data.split("_")[2])
    if call.from_user.id != host_id: 
        return await call.answer("❌ Only the Host can start the race!", show_alert=True)
        
    game = racing_games.get(host_id)
    if not game or game['status'] != 'betting': return await call.answer("❌ Invalid state.", show_alert=True)
    
    if len(game['bets']) == 0:
        return await call.answer("❌ Someone needs to place a bet first!", show_alert=True)
        
    game['status'] = 'racing'
    
    await call.message.edit_text(
        f"🚦 <b>𝗕𝗘𝗧𝗦 𝗔𝗥𝗘 𝗖𝗟𝗢𝗦𝗘𝗗</b>\n"
        f"<i>The engines are revving...</i>",
        parse_mode="HTML"
    )
    
    # START THE ENGINE
    asyncio.create_task(run_racing_sprint(host_id))

async def run_racing_sprint(host_id: int):
    """Manages the live race track updates and calculates the winner."""
    game = racing_games.get(host_id)
    if not game or game['status'] != 'racing': return
    
    theme = RACING_THEMES[game['track']]
    track_length = 15
    positions = [0, 0, 0, 0, 0] # Starting positions for the 5 racers
    
    # Send the initial track
    await asyncio.sleep(2.0)
    
    race_active = True
    winner_idx = None

    while race_active:
        event_log = []
        
        # 1. Calculate Movement & Events
        for i in range(5):
            roll = random.random()
            racer_icon = theme['racers'][i].split()[0]
            
            if roll < 0.15:
                # 🚀 NITROUS BOOST
                positions[i] += 4
                event_log.append(f"🔥 {racer_icon} hits the nitrous!")
            elif roll < 0.30:
                # ⚠️ BLOWOUT / STUMBLE
                positions[i] += 0
                event_log.append(f"⚠️ {racer_icon} stalled out!")
            else:
                # Normal Pace
                positions[i] += random.randint(1, 2)
                
        # 2. Check for Finish Line Crossing
        max_pos = max(positions)
        if max_pos >= track_length:
            race_active = False
            # Find who crossed. If multiple, trigger a photo finish!
            leaders = [i for i, pos in enumerate(positions) if pos == max_pos]
            winner_idx = random.choice(leaders) # Exact winner
            
        # 3. Build the UI
        text = (
            f"🏁 <b>{theme['name'].upper()}</b> 🏁\n"
            f"━━━━━━━━━━━━━━━━━━\n"
        )
        
        for i in range(5):
            icon = theme['racers'][i].split()[0]
            # Cap visual position at the finish line
            vis_pos = min(positions[i], track_length)
            
            dots_before = "." * vis_pos
            dots_after = "." * (track_length - vis_pos)
            text += f"🏆 |{dots_before}{icon}{dots_after}|\n"
            
        text += f"━━━━━━━━━━━━━━━━━━\n🎙️ <b>LIVE:</b> "
        
        if not event_log:
            text += "<i>Neck and neck...</i>"
        else:
            # Show up to 2 events so the UI doesn't stretch too much
            text += f"<i>{' '.join(event_log[:2])}</i>"
            
        if race_active:
            try:
                await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=None, parse_mode="HTML")
            except: pass
            await asyncio.sleep(2.5) # The critical delay to prevent Telegram Rate Limits
        else:
            # The Final Update before the payout
            text = text.replace("🎙️ <b>LIVE:</b>", "📸 <b>PHOTO FINISH:</b>")
            try:
                await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=None, parse_mode="HTML")
            except: pass
            
    # 4. Trigger the Payout
    await asyncio.sleep(2.0)
    await finish_racing_sprint(host_id, winner_idx)


async def finish_racing_sprint(host_id: int, winner_idx: int):
    """Calculates the Parimutuel Winner-Takes-All payouts."""
    game = racing_games.get(host_id)
    if not game: return
    game['status'] = 'finished'

    theme = RACING_THEMES[game['track']]
    winning_racer_name = theme['racers'][winner_idx]
    
    total_pot = game['bet'] * len(game['bets'])
    tax = int(total_pot * 0.05)
    net_pot = total_pot - tax
    
    # Find who bet on the winner
    winning_bettors = [uid for uid, data in game['bets'].items() if data['racer_idx'] == winner_idx]
    losing_bettors = [uid for uid, data in game['bets'].items() if data['racer_idx'] != winner_idx]
    
    text = (
        f"👑 <b>{winning_racer_name} WINS!</b> 👑\n"
        f"━━━━━━━━━━━━━━━━━━\n"
    )
    
    if len(winning_bettors) == 0:
        # THE HOUSE SWEEPS
        text += (
            f"💀 <b>THE HOUSE SWEEPS!</b>\n"
            f"Not a single person backed the winner.\n"
            f"The Casino pockets the entire <b>{total_pot:,} Gold</b> pot!"
        )
    else:
        # THE PLAYERS WIN
        split_cut = net_pot // len(winning_bettors)
        w_names = []
        
        for uid in winning_bettors:
            w_names.append(f"@{game['bets'][uid]['name']}")
            p_win = await get_player_fast(types.User(id=uid, is_bot=False, first_name=""))
            await update_player_balance(uid, split_cut, {"wins": p_win.get('wins', 0) + 1})
            
        text += (
            f"🏆 <b>THE PAYOUT:</b> {net_pot:,} Gold\n"
            f"<i>Split between {len(winning_bettors)} winning tickets.</i>\n\n"
            f"💰 <b>{', '.join(w_names)}</b> each take <b>{split_cut:,} Gold!</b>\n"
            f"<i>(5% House Tax Applied)</i>"
        )
        
    # Log the losers silently
    for uid in losing_bettors:
        p_lose = await get_player_fast(types.User(id=uid, is_bot=False, first_name=""))
        await handle_loss(p_lose)

    # --- ADD THE AUDIT LOG HERE ---
    if len(winning_bettors) == 0:
        asyncio.create_task(send_log(f"🏁 <b>HOUSE SWEEP</b>\nNot a single player backed {winning_racer_name}. The Casino pocketed <b>{total_pot:,} Gold</b>!"))
    else:
        asyncio.create_task(send_log(f"🏁 <b>RACE PAYOUT</b>\n{len(winning_bettors)} players backed {winning_racer_name} and split <b>{net_pot:,} Gold</b>!"))

    try:
        await bot.edit_message_text(text, chat_id=game['chat_id'], message_id=game['msg_id'], reply_markup=None, parse_mode="HTML")
    except: pass

    # Clean RAM
    del racing_games[host_id]

async def main():
    loop = asyncio.get_running_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    loop.set_default_executor(executor)
    
    run_migrations()

    commands = [
        types.BotCommand(command="start", description="🏠 Enter the Vault"),
        types.BotCommand(command="bal", description="💰 Check Gold"),
        types.BotCommand(command="pay", description="💰 share your gold"),
        types.BotCommand(command="daily", description="🎁 Claim & Double Coins"),
        types.BotCommand(command="earn", description="📺 Complete sponsor tasks for gold"),
        types.BotCommand(command="bank", description="🏦 Bank, Interest & Rewards"),
        types.BotCommand(command="deposit", description="📥 Deposit gold to Bank"),
        types.BotCommand(command="withdraw", description="📤 Withdraw gold from Bank"),
        types.BotCommand(command="spin", description="🎡 Daily Lucky Wheel"),
        types.BotCommand(command="refer", description="👥 Invite Friends & Earn"),
        types.BotCommand(command="explore", description="Explore to enter new pubs to get free coins and earn"),
        types.BotCommand(command="exitpub", description="Cash out your pub loots"),
        types.BotCommand(command="buy", description="🪙 Buy Guards "),
        types.BotCommand(command="duel", description="⚔️ Bo3 PvP from your Harem"),
        types.BotCommand(command="draft", description="🌪️ Bo3 PvP from a Combined Harem"),
        types.BotCommand(command="rps", description="🪙 Challenge others for an rps game"),
        types.BotCommand(command="challenge", description="⚔️ Challenge a player to PvP"),
        types.BotCommand(command="aeroplane", description="✈️ Smuggler's Run (Group Crash)"),
        types.BotCommand(command="heist", description="🏦 The Syndicate Heist (Co-op/Betrayal)"),
        types.BotCommand(command="racing", description="🏁 Live Track Betting (Group)"),
        types.BotCommand(command="roulette", description="🔫 Russian Roulette (Group)"),
        types.BotCommand(command="poker", description="🃏 The Standoff (Group Bluffing)"),
        types.BotCommand(command="brawl", description="🪙 Pvp using your team"),
        types.BotCommand(command="coin", description="🪙 Heads or Tails"),
        types.BotCommand(command="slots", description="🎰 Slot Machine"),
        types.BotCommand(command="dart", description="🎯 Hit the Bullseye"),
        types.BotCommand(command="bowling", description="🎳 Roll a Strike"),
        types.BotCommand(command="basket", description="🏀 Shoot Hoops"),
        types.BotCommand(command="football", description="⚽ Kick a Goal"),
        types.BotCommand(command="dice", description="🎲 Under/Over 7"),
        types.BotCommand(command="crates", description="📦 3-9 Crates. One Jackpot."),
        types.BotCommand(command="mines", description="💣 5x5 Minefield"),
        types.BotCommand(command="cards", description="🃏 High or Low"),
        types.BotCommand(command="summon", description="summon characters"),
        types.BotCommand(command="redeem", description="redeem presents if available"),
        types.BotCommand(command="codes", description="🎟️ View all active promo codes"),
        types.BotCommand(command="view", description="view a character"),
        types.BotCommand(command="collect", description="Collect passive gold"),
        types.BotCommand(command="team", description="View your team"),
        types.BotCommand(command="harem", description="View your collection"),
        types.BotCommand(command="equip", description="Equip Characters"),
        types.BotCommand(command="rob", description="🥷 Steal from a User"),
        types.BotCommand(command="bounty", description="🥷 View the active bounty"),
        types.BotCommand(command="stats", description="📊 Your Record"),
        types.BotCommand(command="top", description="🏆 Richest Players"),
    ]

    await bot.set_my_commands(commands)
    await bot.delete_webhook(drop_pending_updates=True)
    
    @dp.shutdown()
    async def on_shutdown():
        logging.warning("🚨 Bot shutting down! Flushing database queue...")
        await send_sys_log("🔴 <b>SYSTEM SHUTTING DOWN</b>\nFlushing RAM queues to database...")
        
        if not PENDING_DB_UPDATES:
            logging.info("✅ Database queue empty. Safe to exit.")
            await send_sys_log("✅ <b>SHUTDOWN COMPLETE</b>\nNo pending updates. Safe exit.")
            return

        tables = list(PENDING_DB_UPDATES.keys())
        for table in tables:
            records = PENDING_DB_UPDATES.pop(table, {})
            for match_val, payload in records.items():
                try:
                    match_col = payload.pop("_match_col")
                    supabase.table(table).update(payload).eq(match_col, match_val).execute()
                except Exception as e:
                    logging.error(f"❌ Failed to save {match_val} during shutdown: {e}")
        logging.info("✅ All pending data saved! Goodbye.")
        await send_sys_log("✅ <b>SHUTDOWN COMPLETE</b>\nAll RAM queues forcefully saved to DB.")

    @dp.error()
    async def global_error_handler(event: ErrorEvent):
        logging.error(f"⚠️ Critical Error: {event.exception}")
        # Use html.escape to prevent angled brackets from breaking Telegram formatting!
        safe_error = html.escape(str(event.exception))
        asyncio.create_task(send_sys_log(f"⚠️ <b>CRITICAL PYTHON ERROR</b>\n<code>{safe_error}</code>"))

    asyncio.create_task(db_worker())
    asyncio.create_task(background_cleanup_task())
    asyncio.create_task(daily_bounty_task())
    asyncio.create_task(auto_backup_task()) # <--- START THE BACKUP TASK
    asyncio.create_task(daily_drop_task())
    
    await load_characters_to_ram() 
    
    logging.info("🤖 Midnight Casino Bot is Online.")
    await send_sys_log("🟢 <b>SYSTEM STARTUP</b>\nMidnight Casino Bot has successfully deployed and connected to the Database.")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
