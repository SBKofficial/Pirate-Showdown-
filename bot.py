import os
import math
import asyncio
import logging
import random
import html
import gc
import json
import concurrent.futures
from datetime import datetime, date, timedelta, timezone
from aiogram import Bot, Dispatcher, types, F

IST = timezone(timedelta(hours=5, minutes=30))

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
MINI_APP_URL = "https://midnight-casino-bot.pages.dev"
MAIN_GROUP_ID = -1003813960922
LOG_GROUP_ID = -1003519381734
SYS_LOG_GROUP_ID = -1003519381734

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
    "Common": 100,
    "Rare": 500,
    "Epic": 2000,
    "Legendary": 10000,
    "Mythic": 50000
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

mine_games = {}      
card_games = {}      
coin_games = {}
dice_games = {}
rps_games = {} 
duel_games = {}   # <--- ADD THIS
draft_games = {}  # <--- ADD THIS
active_games = set() 

CARDS = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]
DAILY_EARNING_LIMIT = 20000000

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
                "daily_earnings": 0,
                "active_defense": "None",
                "defense_health": 0,
                "has_used_ad_spin": False,
                "ad_spin_unlocked": False,
                "in_pub": False,
                "pub_name": None,
                "pub_game": None,
                "pub_balance": 0,
                "pub_plays": 0,
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
            for d in [mine_games, card_games, dice_games, coin_games, rps_games]:
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
    ADD COLUMN IF NOT EXISTS ad_spin_unlocked BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS daily_earnings BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_earning_date TEXT,
    ADD COLUMN IF NOT EXISTS daily_claimed_at TEXT,
    ADD COLUMN IF NOT EXISTS daily_claimed_at TEXT,
    ADD COLUMN IF NOT EXISTS level INTEGER DEFAULT 1,
    ADD COLUMN IF NOT EXISTS xp BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS retro_gold_paid BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_banned BOOLEAN DEFAULT FALSE, -- 👈 ADD THIS LINE

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
            kb_rows.append([types.InlineKeyboardButton(text="💰 OPEN WEB VAULT", web_app=types.WebAppInfo(url=MINI_APP_URL))])
        else:
            kb_rows.append([types.InlineKeyboardButton(text="💰 OPEN WEB VAULT", url=f"https://t.me/{bot_username}?start=vault")])
            
        return text, types.InlineKeyboardMarkup(inline_keyboard=kb_rows)

    elif section == "games":
        text = (
            "🎲 <b>THE CASINO FLOOR</b> 🎲\n"
            "<i>Place your bets and beat the house.</i>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "<code>/coin [amt]</code> — Heads or Tails\n"
            "<code>/slots [amt]</code> — The Slot Machine\n"
            "<code>/dice [amt]</code> — Under/Over 7\n"
            "<code>/mines [amt] [1-24]</code> — 5x5 Minefield\n"
            "<code>/cards [amt]</code> — High or Low\n"
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
            "<code>/daily</code> — Claim & double daily coins\n"
            "<code>/spin</code> — Daily lucky wheel\n"
            "<code>/bank</code> — 5-step reward ladder\n"
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
            "<code>/rob</code> — Steal gold (Reply to a user)\n"
            "<code>/bounty</code> — View the active bounty\n"
            "<code>/buy</code> — Buy Guards/Dogs to block robberies\n"
            "<code>/duel [amt]</code> — Bo3 PvP using your own Harem\n"
            "<code>/draft [amt]</code> — Bo3 PvP using a shared deck\n"
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


async def check_daily_limit(m: types.Message, p: dict) -> bool:
    today = str(datetime.now(IST).date())
    
    if p.get('last_earning_date') != today:
        p['daily_earnings'] = 0
        p['last_earning_date'] = today
        queue_db_update("players", {"daily_earnings": 0, "last_earning_date": today}, "id", p['id'])

    if p.get('daily_earnings', 0) >= DAILY_EARNING_LIMIT:
        if m.chat.type == "private":
            kb = types.InlineKeyboardMarkup(inline_keyboard=[[
                types.InlineKeyboardButton(text="📺 Watch Ad to Reset Limit", web_app=types.WebAppInfo(url=MINI_APP_URL))
            ]])
        else:
            bot_user = await bot.get_me()
            kb = types.InlineKeyboardMarkup(inline_keyboard=[[
                types.InlineKeyboardButton(text="📺 RESET LIMIT IN PRIVATE", url=f"https://t.me/{bot_user.username}?start=limit")
            ]])
            
        await m.answer(
            f"🛑 <b>DAILY LIMIT REACHED!</b> 🛑\n\n"
            f"You have won over <b>{DAILY_EARNING_LIMIT:,} gold</b> today (Net Profit).\n"
            f"Watch an ad to reset your limit and keep playing!", 
            reply_markup=kb, parse_mode="HTML"
        )
        return False
    return True

async def process_win(p: dict, win_amount: int, bet_amount: int):
    today = str(datetime.now(IST).date())
    house_cut = int(win_amount * 0.05)
    player_payout = win_amount - house_cut
    
    net_profit = player_payout - bet_amount
    limit_increment = max(0, net_profit) 
    
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
            
    p['xp'] = new_xp
    # -----------------------
    
    updates = {
        "wins": p.get('wins', 0) + 1,
        "daily_earnings": p.get('daily_earnings', 0) + limit_increment,
        "last_earning_date": today,
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

async def get_spin_ad_kb(chat_type):
    if chat_type == "private":
        return types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="📺 Watch Ad for +1 Spin", web_app=types.WebAppInfo(url=MINI_APP_URL))
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
    if m.from_user.id != 7708811819: return

    args = command.args
    if not args:
        return await m.answer("❌ <b>Format:</b>\n`/addchar Name | Anime | Rarity | Image_URL`", parse_mode="HTML")
    
    try:
        parts = [p.strip() for p in args.split("|")]
        if len(parts) != 4:
            return await m.answer("❌ <b>Format Error!</b>", parse_mode="HTML")
        
        name, anime, rarity, image = parts[0], parts[1], parts[2].capitalize(), parts[3]
        
        if rarity not in RARITY_RATES:
            return await m.answer(f"❌ <b>Invalid rarity!</b>", parse_mode="HTML")
            
        # --- UPSERT LOGIC ---
        # This tells Supabase: "If 'name' already exists, update the other fields"
        res = await async_db(supabase.table("characters").upsert({
            "name": name, 
            "anime": anime, 
            "rarity": rarity, 
            "image": image
        }, on_conflict="name")) # <--- Crucial part
        
        if not res.data:
            return await m.answer("❌ Database error.")

        # Update RAM Cache
        new_char = res.data[0]
        CHARACTER_CACHE[new_char['id']] = new_char
        
        # Check if it was an update or a new add
        msg = "Character Added!" if len(res.data) > 0 else "Character Updated!"
        
        await m.answer_photo(
            photo=image, 
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

    if target_id in coin_games:
        refund = coin_games.pop(target_id)['bet']
        game_type = "Coin Flip"
    elif target_id in dice_games:
        refund = dice_games.pop(target_id)['bet']
        game_type = "Dice"
    elif target_id in card_games:
        game = card_games.pop(target_id)
        refund = game['bet']
        game_type = "Cards"
    elif target_id in mine_games:
        game = mine_games.pop(target_id)
        refund = game['bet']
        game_type = "Mines"
    elif target_id in rps_games:
        game = rps_games.pop(target_id)
        refund = game['bet']
        game_type = "RPS"

    active_games.discard(target_id)

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
    else:
        await m.answer(f"❌ User {target_id} has no stuck games.", parse_mode="HTML")

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
        else:
            await update_player_balance(game['challenger'], game['bet'])
            await update_player_balance(game['defender'], game['bet'])
            await call.message.edit_text(f"{text}\n\n🤝 <b>GAME DRAW!</b>\nGold refunded to both.", parse_mode="HTML")
        
        del rps_games[game_id]
# --- 13. GACHA PVP (DUEL & DRAFT BO3) ---
async def expire_pvp_challenge(game_dict, challenger_id, msg: types.Message):
    """Automatically cancels the challenge if not accepted in 60 seconds."""
    await asyncio.sleep(60)
    if challenger_id in game_dict:
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
        
        # Increment the usage count globally
        await async_db(supabase.table("promo_codes").update({"uses": promo['uses'] + 1}).eq("code", code))
        
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
        global BOUNTY_TARGET_ID, BOUNTY_AMOUNT
        
        # 1. Check if the bounty is empty (claimed or not set)
        if not BOUNTY_TARGET_ID:
            return await m.answer(
                "🎯 <b>BOUNTY BOARD: EMPTY</b>\n\n"
                "The streets are quiet. The last bounty was claimed or expired.\n"
                "<i>Wait for the Midnight reset for a new target.</i>", 
                parse_mode="HTML"
            )
            
        # 2. If active, fetch the target's latest name
        res = await async_db(supabase.table("players").select("username").eq("id", BOUNTY_TARGET_ID))
        
        if res.data:
            target_name = html.escape(res.data[0].get('username') or f"User_{BOUNTY_TARGET_ID}")
            target_tag = f"<a href='tg://user?id={BOUNTY_TARGET_ID}'>@{target_name}</a>"
            
            await m.answer(
                f"🎯 <b>ACTIVE BOUNTY</b> 🎯\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"🩸 <b>Target:</b> {target_tag}\n"
                f"💰 <b>Reward:</b> {BOUNTY_AMOUNT:,} gold\n\n"
                f"<i>The first person to successfully /rob them claims this bonus from the House!</i>",
                parse_mode="HTML"
            )
        else:
            await m.answer("🎯 <b>ACTIVE BOUNTY</b>\nThe target is currently in hiding...", parse_mode="HTML")
            
    except Exception as e:
        await m.answer(f"❌ Error checking the bounty board: {e}")
    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("start"))
async def cmd_start(m: types.Message, command: CommandObject = None):
    referrer_id = None
    args = command.args
    
    # Handle Deep Links (Buttons from groups that send them to DMs)
    if args:
        if args == "spin": return await cmd_spin(m)
        elif args == "daily": return await cmd_daily(m)
        elif args == "bank": return await cmd_bank(m)
        elif args == "limit": return await cmd_stats(m)
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
                types.InlineKeyboardButton(text="📺 Claim 500 (Watch Ad)", web_app=types.WebAppInfo(url=MINI_APP_URL))
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
        
        if p.get('last_bank_reset') != today_str:
            p['bank_ads_count'] = 0
            p['last_bank_reset'] = today_str
            queue_db_update("players", {"bank_ads_count": 0, "last_bank_reset": today_str}, "id", m.from_user.id)

        count = p.get('bank_ads_count', 0)
        if count >= 5:
            return await m.answer("🏦 <b>Bank is full!</b>\nAll daily tiers claimed.", parse_mode="HTML")

        reward = (count + 1) * 100
        if m.chat.type == "private":
            kb = types.InlineKeyboardMarkup(inline_keyboard=[[
                types.InlineKeyboardButton(text=f"📺 Tier {count+1}/5 ({reward} coins)", web_app=types.WebAppInfo(url=MINI_APP_URL))
            ]])
        else:
            bot_user = await bot.get_me()
            kb = types.InlineKeyboardMarkup(inline_keyboard=[[
                types.InlineKeyboardButton(text="🏦 CLAIM AT THE BANK", url=f"https://t.me/{bot_user.username}?start=bank")
            ]])
        await m.answer(f"🏦 <b>Bank Ladder ({count}/5)</b>\nWatch to unlock Tier {count+1}.", reply_markup=kb, reply_to_message_id=m.message_id, parse_mode="HTML")
    finally:
        active_games.discard(m.from_user.id)

@dp.message(Command("stats"))
async def cmd_stats(m: types.Message):
    p = await get_player_fast(m.from_user)
    wins = p.get('wins', 0)
    losses = p.get('total_losses', 0)
    robs_won = p.get('robs_won', 0)
    robs_lost = p.get('robs_lost', 0)
    
    today_str = str(datetime.now(IST).date())
    daily_earnings = p.get('daily_earnings', 0)
    if p.get('last_earning_date') != today_str:
        daily_earnings = 0
    
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
        f"💰 <b>Daily Limit:</b> {daily_earnings:,} / {DAILY_EARNING_LIMIT:,}\n"
        f"<i>(Resets at Midnight IST)</i>\n\n"
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
        
    # --- THE FIX: Block robbing bots ---
    if victim.is_bot:
        return await m.answer("❌ You cannot rob the House (or any other bot)!")
    # -----------------------------------

    if m.from_user.id in active_games or victim_id in active_games:
        return await m.answer("⏳ Please wait, someone is busy processing a game!")
    active_games.add(m.from_user.id)
    active_games.add(victim_id)

    try:
        robber = await get_player_fast(m.from_user)
        victim = await get_player_fast(m.reply_to_message.from_user)
        
        now = datetime.now(IST) 
        if robber.get('last_rob_time'):
            last_rob = datetime.fromisoformat(robber['last_rob_time'].replace('Z', '+00:00'))
            if now < last_rob + timedelta(hours=12):
                wait_time = (last_rob + timedelta(hours=12)) - now
                return await m.answer(f"⏳ Police are patrolling! Wait {wait_time.seconds // 60}m.", reply_to_message_id=m.message_id)

        robber['last_rob_time'] = now.isoformat()
        queue_db_update("players", {"last_rob_time": now.isoformat()}, "id", m.from_user.id)

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
                    await bot.send_message(victim['id'], f"⚠️ Your <b>{defense}</b> was used up protecting you from @{safe_robber}!")
                except Exception as e: 
                    logging.warning(f"Notification error: {e}")
            else:
                victim['defense_health'] = health
                queue_db_update("players", {"defense_health": health}, "id", victim['id'])
        
            safe_victim = html.escape(m.reply_to_message.from_user.username or m.reply_to_message.from_user.first_name)
            safe_robber = html.escape(m.from_user.username or m.from_user.first_name)
            
            # --- NEW: SEND AUDIT LOG (CAUGHT) ---
            asyncio.create_task(send_log(f"🚨 <b>ROBBERY FAILED</b>\n<a href='tg://user?id={m.from_user.id}'>{safe_robber}</a> tried to rob <a href='tg://user?id={victim['id']}'>{safe_victim}</a> but was caught by a {defense}! (Fined: {fine:,})"))

            return await m.answer(
                f"🚓 <b>POLICE CAUGHT YOU!</b>\n\n"
                f"@{safe_victim} was protected by a <b>{defense}</b>.\n"
                f"You were fined <b>{fine:,} gold</b>.", parse_mode="HTML"
            )

        if random.random() > 0.5:
            steal_amount = int(victim['balance'] * 0.10)
            robber['robs_won'] = robber.get('robs_won', 0) + 1
            
            # --- BOUNTY CLAIM LOGIC ---
            global BOUNTY_TARGET_ID, BOUNTY_AMOUNT
            bounty_bonus_text = ""
            log_bounty_tag = ""
            
            if victim['id'] == BOUNTY_TARGET_ID:
                await update_player_balance(m.from_user.id, BOUNTY_AMOUNT)
                bounty_bonus_text = f"\n\n💰 <b>BOUNTY CLAIMED!</b> The House paid you an extra <b>{BOUNTY_AMOUNT:,} gold</b> for the hit!"
                log_bounty_tag = f"\n🎯 <b>BOUNTY CLAIMED: +{BOUNTY_AMOUNT:,} Gold!</b>"
                BOUNTY_TARGET_ID = None # Clear for the day
                BOUNTY_AMOUNT = 0
            
            await update_player_balance(victim['id'], -steal_amount)
            await update_player_balance(m.from_user.id, steal_amount, {"robs_won": robber['robs_won']})
            
            safe_victim = html.escape(m.reply_to_message.from_user.username or m.reply_to_message.from_user.first_name)
            safe_robber = html.escape(m.from_user.username or m.from_user.first_name)
            
            # --- NEW: SEND AUDIT LOG (SUCCESS) ---
            asyncio.create_task(send_log(f"🥷 <b>ROBBERY SUCCESS</b>\n<a href='tg://user?id={m.from_user.id}'>{safe_robber}</a> stole <b>{steal_amount:,} Gold</b> from <a href='tg://user?id={victim['id']}'>{safe_victim}</a>!{log_bounty_tag}"))

            await m.answer(f"🥷 <b>Success!</b> You stole <b>{steal_amount:,} gold</b> from {safe_victim}!{bounty_bonus_text}", parse_mode="HTML")

        else:
            robber['robs_lost'] = robber.get('robs_lost', 0) + 1
            queue_db_update("players", {"robs_lost": robber['robs_lost']}, "id", m.from_user.id)
            await m.answer("🤫 You tripped and fell! The robbery failed.")

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
        if not await check_daily_limit(m, p):
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
            display_name = f"<a href='tg://user?id={player['id']}'>{raw_name}</a>"
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
            display_name = f"<a href='tg://user?id={player['id']}'>{raw_name}</a>"
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
            kb = await get_spin_ad_kb(m.chat.type)
            await m.answer("🎰 <b>Daily spin complete!</b>\nWatch an ad to unlock <b>one more spin!</b>", reply_markup=kb, parse_mode="HTML")
            return

        if p.get('ad_spin_unlocked'):
            p['ad_spin_unlocked'] = False
            p['has_used_ad_spin'] = True
            queue_db_update("players", {"ad_spin_unlocked": False, "has_used_ad_spin": True}, "id", m.from_user.id)
            await execute_spin(m, p)
            return

        kb = await get_spin_ad_kb(m.chat.type)
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
    if m.from_user.id in active_games: return await m.answer("⏳ Wait...")
    active_games.add(m.from_user.id)
    try:
        if not CHARACTER_CACHE:
            return await m.answer("❌ The Gacha pool is currently empty. Tell the Admin to add characters!")
            
        p = await get_player_fast(m.from_user)
        cost = 500000
        
        if p['balance'] < cost:
            return await m.answer(f"❌ You need <b>{cost:,} gold</b> to summon.", parse_mode="HTML")
          

        # Roll RNG (0 to 100)
        roll = random.random() * 100
        
        if roll <= 0.01: 
            target_rarity = "Mythic"       # 0.01% chance (Impossible)
        elif roll <= 1.0: 
            target_rarity = "Legendary"    # 0.99% chance (1.0 - 0.01)
        elif roll <= 8.0: 
            target_rarity = "Epic"         # 7.0% chance (8.0 - 1.0)
        elif roll <= 30.0: 
            target_rarity = "Rare"         # 22.0% chance (30.0 - 8.0)
        else: 
            target_rarity = "Common"       # 70.0% chance

        # Find characters matching the rolled rarity
        pool = [c for c in CHARACTER_CACHE.values() if c['rarity'] == target_rarity]
        
        # Fallback if the admin hasn't added any characters of that rarity yet
        if not pool:
            pool = list(CHARACTER_CACHE.values())
            
        char = random.choice(pool)
        owned = p.get('owned_characters') or []
        
        # Emoji Map
        emojis = {"Common": "🤍", "Rare": "💙", "Epic": "💜", "Legendary": "💛", "Mythic": "💖"}
        e = emojis.get(char['rarity'], "✨")
        
        caption = f"{e} <b>{char['rarity'].upper()} PULL!</b> {e}\n\n🌸 <b>{char['name']}</b>\n📺 {char['anime']}\n💰 Base Income: {RARITY_RATES[char['rarity']]:,}/hr"
        
        if char['id'] in owned:
            # Duplicate Refund (20%)
            refund = int(cost * 0.2)
            await update_player_balance(m.from_user.id, -cost + refund)
            caption += f"\n\n⚠️ <i>You already own this character!</i>\n💸 Refunded: <b>{refund:,} gold</b>"
            await m.answer_photo(photo=char['image'], caption=caption, parse_mode="HTML")
            
            # --- NEW: SEND AUDIT LOG (DUPE) ---
            safe_name = html.escape(m.from_user.first_name)
            asyncio.create_task(send_log(f"♻️ <b>GACHA (DUPLICATE)</b>\n<a href='tg://user?id={m.from_user.id}'>{safe_name}</a> pulled a duplicate {e} <b>{char['name']}</b> [{char['rarity']}]"))
            
        else:
            # New Character!
            owned.append(char['id'])
            updates = {"owned_characters": owned}
            if not p.get('last_collect_time'):
                updates['last_collect_time'] = datetime.now(IST).isoformat()
                
            await update_player_balance(m.from_user.id, -cost, updates)
            p['owned_characters'] = owned # Update RAM
            
            caption += "\n\n🎉 <b>NEW CHARACTER UNLOCKED!</b>\nUse /team and /equip to use them!"
            await m.answer_photo(photo=char['image'], caption=caption, parse_mode="HTML")
            
            # --- NEW: SEND AUDIT LOG (NEW) ---
            safe_name = html.escape(m.from_user.first_name)
            alert_icon = "🚨" if char['rarity'] in ["Legendary", "Mythic"] else "🌸"
            asyncio.create_task(send_log(f"{alert_icon} <b>GACHA (NEW)</b>\n<a href='tg://user?id={m.from_user.id}'>{safe_name}</a> unlocked {e} <b>{char['name']}</b> [{char['rarity']}]!"))

    finally:
        active_games.discard(m.from_user.id)

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


@dp.message(F.web_app_data)
async def handle_rewards(m: types.Message):
    if m.web_app_data.data == "AD_WATCH_COMPLETE":
        if m.from_user.id in active_games: return await m.answer("⏳ Please wait...")
        active_games.add(m.from_user.id)
        
        try:
            p = await get_player_fast(m.from_user)
            today_str = str(datetime.now(IST).date())
            
            if p.get('daily_earnings', 0) >= DAILY_EARNING_LIMIT:
                p['daily_earnings'] = 0
                p['last_earning_date'] = today_str
                queue_db_update("players", {"daily_earnings": 0, "last_earning_date": today_str}, "id", m.from_user.id)
                return await m.answer("✅ <b>LIMIT RESET!</b>\nYour daily earning limit has been refreshed. Get back to the games!", parse_mode="HTML")

            if p.get('last_spin_date') == today_str and not p.get('has_used_ad_spin') and not p.get('ad_spin_unlocked'):
                p['ad_spin_unlocked'] = True
                queue_db_update("players", {"ad_spin_unlocked": True}, "id", m.from_user.id)
                return await m.answer("✅ <b>Bonus Spin Unlocked!</b>\nType /spin to use it!", parse_mode="HTML")

            claimed_at = p.get('daily_claimed_at', '') or ''
            daily_ready = not claimed_at.startswith(today_str)

            if daily_ready:
                now_iso = datetime.now(IST).isoformat()
                p['daily_claimed_at'] = now_iso
                await update_player_balance(m.from_user.id, 500, {"daily_claimed_at": now_iso})
                
                if m.chat.type == "private":
                    double_kb = types.InlineKeyboardMarkup(inline_keyboard=[[
                        types.InlineKeyboardButton(text="🔥 DOUBLE IT (Watch 2nd Ad)", web_app=types.WebAppInfo(url=MINI_APP_URL))
                    ]])
                else:
                    bot_user = await bot.get_me()
                    double_kb = types.InlineKeyboardMarkup(inline_keyboard=[[
                        types.InlineKeyboardButton(text="🔥 DOUBLE IT IN PRIVATE", url=f"https://t.me/{bot_user.username}?start=daily")
                    ]])
                await m.answer("✅ <b>500 Coins Collected!</b>\nWant to turn it into <b>1,000</b>? Watch one more!", reply_markup=double_kb, parse_mode="HTML")
            else:
                count = p.get('bank_ads_count', 0)
                if count < 5:
                    reward = (count + 1) * 100
                    await update_player_balance(m.from_user.id, reward, {"bank_ads_count": count + 1})
                    await m.answer(f"✅ <b>Tier {count+1} Unlocked!</b>\n+{reward} coins added.")
                else:
                    await m.answer("🏦 Your daily bank tiers are exhausted!")
        finally:
            active_games.discard(m.from_user.id)

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
        types.BotCommand(command="bank", description="🏦 5-Step Reward Ladder"),
        types.BotCommand(command="spin", description="🎡 Daily Lucky Wheel"),
        types.BotCommand(command="refer", description="👥 Invite Friends & Earn"),
        types.BotCommand(command="explore", description="Explore to enter new pubs to get free coins and earn"),
        types.BotCommand(command="exitpub", description="Cash out your pub loots"),
        types.BotCommand(command="buy", description="🪙 Buy Guards "),
        types.BotCommand(command="duel", description="⚔️ Bo3 PvP from your Harem"),
        types.BotCommand(command="draft", description="🌪️ Bo3 PvP from a Combined Harem"),
        types.BotCommand(command="rps", description="🪙 Challenge others for an rps game"),
        types.BotCommand(command="coin", description="🪙 Heads or Tails"),
        types.BotCommand(command="slots", description="🎰 Slot Machine"),
        types.BotCommand(command="dice", description="🎲 Under/Over 7"),
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
        # Automatically alert you of Python crashes!
        asyncio.create_task(send_sys_log(f"⚠️ <b>CRITICAL PYTHON ERROR</b>\n<code>{event.exception}</code>"))
        
    asyncio.create_task(db_worker())
    asyncio.create_task(background_cleanup_task())
    asyncio.create_task(daily_bounty_task())
    asyncio.create_task(auto_backup_task()) # <--- START THE BACKUP TASK

    await load_characters_to_ram() 
    
    logging.info("🤖 Midnight Casino Bot is Online.")
    await send_sys_log("🟢 <b>SYSTEM STARTUP</b>\nMidnight Casino Bot has successfully deployed and connected to the Database.")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
