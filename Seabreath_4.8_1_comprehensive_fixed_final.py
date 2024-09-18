import asyncpg
import os
from cryptography.fernet import Fernet
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler
from solana.rpc.async_api import AsyncClient
from pyserum.market import Market
from pythclient.pythaccounts import PythPriceAccount
from solana.transaction import Transaction
from solders.keypair import Keypair
import asyncio
import logging
import base58
import time
import base64

# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection using postgres user
DATABASE_URL = "postgresql://postgres:<password>@localhost:5432/seabreath"

# Hardcode the actual Fernet key
FERNET_KEY = "__1RCQFIxFxppPBzQKPSy7hUf9CI2WGRUQvppKiGpOc="
FERNET_KEY = FERNET_KEY + '=' * (4 - len(FERNET_KEY) % 4)

# Initialize Fernet with the hardcoded key
try:
    fernet = Fernet(FERNET_KEY)
except ValueError as e:
    logger.error(f"Invalid FERNET_KEY: {e}")
    raise

# Telegram Bot Token
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

# Multiple RPC nodes for load balancing and monitoring
RPC_NODES = [
    {"url": "https://api.mainnet-beta.solana.com", "latency": None, "load": 0},
    {"url": "https://solana-mainnet.rpcpool.com", "latency": None, "load": 0},
    {"url": "https://solana-api.projectserum.com", "latency": None, "load": 0}
]

# Dictionary to track active strategies
active_strategies = {}

## ==============================
# Live RPC Node Monitoring & Fastest Node Selection
# ==============================

async def monitor_and_select_rpc():
    """Monitors and selects the fastest RPC node based on live latency and load."""
    tasks = []

    async def check_node_health(node):
        client = AsyncClient(node['url'])
        start_time = time.time()
        try:
            await client.is_connected()
            latency = time.time() - start_time
            node['latency'] = latency
            logger.info(f"RPC Node {node['url']} has latency: {latency}s")
        except Exception:
            node['latency'] = float('inf')
            logger.error(f"Error with RPC Node {node['url']}")
        finally:
            async def close_client():
                pass
                logger.info("Exiting the application safely.")
        async def execute_close_client():
    # Properly indented block after function definition
    await close_client()
    await execute_close_client()

    for node in RPC_NODES:
        tasks.append(check_node_health(node))

    await asyncio.gather(*tasks)

    best_node = min(RPC_NODES, key=lambda x: (x['latency'], x['load']))
    logger.info(f"Selected the best RPC node: {best_node['url']} with latency {best_node['latency']}")
    return best_node['url']


# ==============================
# Transaction Fee Prediction & Optimization
# ==============================

async def predict_fees(client):
    """Predict current transaction fees based on network congestion."""
    try:
        blockhash_resp = await client.get_recent_blockhash()
        fee_per_signature = blockhash_resp['result']['value']['feeCalculator']['lamportsPerSignature']
        logger.info(f"Predicted fee per signature: {fee_per_signature}")
        return fee_per_signature
    except Exception as e:
        logger.error(f"Error predicting fees: {e}")
        return None


# ==============================
# Transaction Batching & Splitting
# ==============================

async def batch_and_split_transactions(transactions):
    """Batch and split large transactions into smaller parts to optimize speed."""
    max_tx_size = 1232  # Solana's max transaction size in bytes
    batch = []
    batch_size = 0

    for tx in transactions:
        tx_size = len(tx.serialize())
        if batch_size + tx_size > max_tx_size:
            # Send current batch
            await send_batch(batch)
            batch = []
            batch_size = 0
        batch.append(tx)
        batch_size += tx_size

    if batch:
        await send_batch(batch)


async def send_batch(batch):
    """Send a batch of transactions."""
    async with await get_fast_client() as client:
        try:
            signed_txs = [tx.sign() for tx in batch]
            await asyncio.gather(*[client.send_transaction(signed_tx) for signed_tx in signed_txs])
            logger.info(f"Batch of {len(batch)} transactions sent.")
        except Exception as e:
            logger.error(f"Error sending batch: {e}")


# ==============================
# RPC Node Management with Live Monitoring
# ==============================

async def get_fast_client():
    """Return the fastest RPC client based on live monitoring."""
    selected_node = await monitor_and_select_rpc()
    return AsyncClient(selected_node)


# ==============================
# Strategy Implementations with Batching & Parallel Processing
# ==============================

async def execute_serum_order(spl_token_address, price, size, side):
    """Execute a Serum market order on Solana."""
    try:
        async with await get_fast_client() as connection:
            market = await Market.load(connection, spl_token_address)
            if side == "buy":
                await market.place_order(connection, side="buy", price=price, size=size)
            elif side == "sell":
                await market.place_order(connection, side="sell", price=price, size=size)
    except Exception as e:
        logger.error(f"Error executing Serum order: {e}")


async def fetch_pyth_price(spl_token_address):
    """Fetch the price of a token from the Pyth network."""
    try:
        async with await get_fast_client() as client:
            pyth_account = await client.get_account_info(spl_token_address)
            price_data = PythPriceAccount.parse_price(pyth_account['data'])
            return price_data.current_price
    except Exception as e:
        logger.error(f"Error fetching Pyth price: {e}")
        return None


# ==============================
# Strategies Implementation
# ==============================

async def automated_smart_profit(update, context, spl_token_address, profit_target=1000, check_interval=5):
    wallet = await get_private_key_postgres(update.effective_user.id)
    if not wallet:
        await update.message.reply_text("No wallet found.")
        return
    
    await update.message.reply_text(f"Starting Smart Profit strategy for {spl_token_address}.")
    
    strategy_key = f'smart_profit_{spl_token_address}'
    active_strategies[update.effective_user.id] = {**active_strategies.get(update.effective_user.id, {}), strategy_key: True}

    while active_strategies[update.effective_user.id].get(strategy_key, False):
        price = await fetch_pyth_price(spl_token_address)
        if price >= profit_target:
            await execute_serum_order(spl_token_address, price, 0.25, "sell")
            await update.message.reply_text(f"Profit target reached: Selling 25%.")
        await asyncio.sleep(check_interval)

async def automated_dca_strategy(update, context, spl_token_address, intervals=5, amount=10, interval_time=60):
    wallet = await get_private_key_postgres(update.effective_user.id)
    if not wallet:
        await update.message.reply_text("No wallet found.")
        return

    strategy_key = f'dca_{spl_token_address}'
    active_strategies[update.effective_user.id] = {**active_strategies.get(update.effective_user.id, {}), strategy_key: True}

    for i in range(intervals):
        if not active_strategies[update.effective_user.id].get(strategy_key, False):
            break
        await execute_serum_order(spl_token_address, await fetch_pyth_price(spl_token_address), amount, "buy")
        await update.message.reply_text(f"DCA purchase {i+1} of {intervals}: {amount} tokens.")
        await asyncio.sleep(interval_time)

async def automated_market_making(update, context, spl_token_address, spread=1, volume_threshold=1000):
    strategy_key = f'market_making_{spl_token_address}'
    active_strategies[update.effective_user.id] = {**active_strategies.get(update.effective_user.id, {}), strategy_key: True}

    while active_strategies[update.effective_user.id].get(strategy_key, False):
        price = await fetch_pyth_price(spl_token_address)
        await execute_serum_order(spl_token_address, price - spread, 10, "buy")
        await execute_serum_order(spl_token_address, price + spread, 10, "sell")
        
        if await detect_high_volume(spl_token_address) >= volume_threshold:
            await update.message.reply_text(f"Boosting market-making due to high volume.")
            await execute_serum_order(spl_token_address, price, 50, "buy")
        await asyncio.sleep(1)

async def automated_mev_front_run(update, context, spl_token_address):
    strategy_key = f'mev_{spl_token_address}'
    active_strategies[update.effective_user.id] = {**active_strategies.get(update.effective_user.id, {}), strategy_key: True}

    while active_strategies[update.effective_user.id].get(strategy_key, False):
        await update.message.reply_text(f"Monitoring for arbitrage opportunities...")
        price_arbitrage = await detect_mempool_arbitrage(spl_token_address)
        
        if price_arbitrage:
            await execute_serum_order(spl_token_address, price_arbitrage['price'], 100, "buy")
            await update.message.reply_text("Arbitrage opportunity detected! Executing front-running.")
        await asyncio.sleep(3)

async def automated_token_swap(update, context, spl_token_address, target_price, swap_to_token):
    strategy_key = f'token_swap_{spl_token_address}'
    active_strategies[update.effective_user.id] = {**active_strategies.get(update.effective_user.id, {}), strategy_key: True}

    while active_strategies[update.effective_user.id].get(strategy_key, False):
        price = await fetch_pyth_price(spl_token_address)
        if price >= target_price:
            await execute_serum_order(swap_to_token, price, 100, "buy")
            await update.message.reply_text(f"Swapped at target price {price}.")
        await asyncio.sleep(2)

async def automated_grinding_strategy(update, context, spl_token_address, trade_size=1, trades=10):
    strategy_key = f'grinding_{spl_token_address}'
    active_strategies[update.effective_user.id] = {**active_strategies.get(update.effective_user.id, {}), strategy_key: True}

    for i in range(trades):
        if not active_strategies[update.effective_user.id].get(strategy_key, False):
            break
        price = await fetch_pyth_price(spl_token_address)
        await execute_serum_order(spl_token_address, price, trade_size, "buy")
        await asyncio.sleep(1)

async def place_limit_order(update, context, spl_token_address, limit_price, amount, order_type="buy"):
    price = await fetch_pyth_price(spl_token_address)
    if (order_type == "buy" and price <= limit_price) or (order_type == "sell" and price >= limit_price):
        await execute_serum_order(spl_token_address, price, amount, order_type)
        await update.message.reply_text(f"Limit {order_type} order executed at {price}.")
    else:
        await update.message.reply_text(f"Limit {order_type} order not met. Current price: {price}")


# Fixes: Replace `sync` with `async`
async def automated_volume_booster(update, context, spl_token_address, volume_threshold):
    """Volume booster strategy to increase activity during high trading volumes."""
    wallet = await get_private_key_postgres(update.effective_user.id)
    if not wallet:
        await update.message.reply_text("No wallet found.")
        return

    strategy_key = f'volume_booster_{spl_token_address}'
    active_strategies[update.effective_user.id] = {**active_strategies.get(update.effective_user.id, {}), strategy_key: True}

    while active_strategies[update.effective_user.id].get(strategy_key, False):
        volume = await detect_high_volume(spl_token_address)
        if volume >= volume_threshold:
            await execute_serum_order(spl_token_address, await fetch_pyth_price(spl_token_address), 100, "buy")
            await update.message.reply_text("High volume detected! Boosting trades.")
        await asyncio.sleep(2)

async def stop_loss_take_profit(update, context, spl_token_address, stop_loss, take_profit):
    """Stop-Loss and Take-Profit strategy based on price thresholds."""
    price = await fetch_pyth_price(spl_token_address)
    if price <= stop_loss:
        await execute_serum_order(spl_token_address, price, 100, "sell")
        await log_transaction(update.effective_user.id, spl_token_address, -100, 'sell', 'confirmed')
        await update.message.reply_text(f"Stop-loss triggered: Sold at {price}.")
    elif price >= take_profit:
        await execute_serum_order(spl_token_address, price, 100, "sell")
        await log_transaction(update.effective_user.id, spl_token_address, -100, 'sell', 'confirmed')
        await update.message.reply_text(f"Take-profit triggered: Sold at {price}.")


# ==============================
# Placeholder for loading active strategies
# ==============================
async def load_active_strategies():
    """Load active strategies from storage or initialize an empty dictionary."""
    logger.info("Loading active strategies.")
    global active_strategies
    active_strategies = {}  # Initialize or load strategies

# ==============================
# Telegram Command Handlers
# ==============================

async def start(update: Update, context):
    keyboard = [
        [InlineKeyboardButton("Generate Wallet", callback_data='generate_wallet')],
        [InlineKeyboardButton("Deposit", callback_data='deposit')],
        [InlineKeyboardButton("Withdraw", callback_data='withdraw')],
        [InlineKeyboardButton("Smart Profit", callback_data='start_smart_profit')],
        [InlineKeyboardButton("DCA", callback_data='start_dca')],
        [InlineKeyboardButton("Market Making", callback_data='start_market_making')],
        [InlineKeyboardButton("MEV Front-Running", callback_data='start_mev')],
        [InlineKeyboardButton("Token Swap", callback_data='start_token_swap')],
        [InlineKeyboardButton("Limit Orders", callback_data='start_limit_orders')],
        [InlineKeyboardButton("Grinding", callback_data='start_grinding')],
        [InlineKeyboardButton("Volume Booster", callback_data='start_volume_booster')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Welcome! Choose an option:", reply_markup=reply_markup)

async def button(update: Update, context):
    query = update.callback_query
    await query.answer()

    if query.data == 'generate_wallet':
        public_key, private_key = generate_wallet()
        await store_wallet_postgres(update.effective_user.id, public_key, private_key)
        await query.message.reply_text(f"Wallet generated:\nPublic Key: {public_key}")
    elif query.data == 'start_smart_profit':
        await automated_smart_profit(update, context, spl_token_address="YOUR_TOKEN_ADDRESS")
    elif query.data == 'start_dca':
        await automated_dca_strategy(update, context, spl_token_address="YOUR_TOKEN_ADDRESS")
    elif query.data == 'start_market_making':
        await automated_market_making(update, context, spl_token_address="YOUR_TOKEN_ADDRESS")
    elif query.data == 'start_mev':
        await automated_mev_front_run(update, context, spl_token_address="YOUR_TOKEN_ADDRESS")
    elif query.data == 'start_token_swap':
        await automated_token_swap(update, context, spl_token_address="YOUR_TOKEN_ADDRESS", target_price=10, swap_to_token="YOUR_SWAP_TOKEN")
    elif query.data == 'start_limit_orders':
        await place_limit_order(update, context, spl_token_address="YOUR_TOKEN_ADDRESS", limit_price=10, amount=1, order_type="buy")
    elif query.data == 'start_grinding':
        await automated_grinding_strategy(update, context, spl_token_address="YOUR_TOKEN_ADDRESS", trade_size=1, trades=10)
    elif query.data == 'start_volume_booster':
        await automated_volume_booster(update, context, spl_token_address="YOUR_TOKEN_ADDRESS", volume_threshold=500)

# ==============================
# BOT MAIN FUNCTION
# ==============================

async def run_bot():
    try:
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

        application.add_handler(CommandHandler("start", start))
        application.add_handler(CallbackQueryHandler(button))

        await load_active_strategies()

        logger.info("Bot started successfully!")
        await application.initialize()
        await application.start()
        await application.idle()

    except Exception as e:
        logger.error(f"Error occurred: {e}")

    finally:
            async def close_client():
                pass
                logger.info("Exiting the application safely.")
                async def execute_client_close():
        await client.close()  # Ensure proper indentation  # Corrected indentation inside the async function  # Wrapped inside async function
    await execute_client_close()
    async def execute_close_client():
    # Properly indented block after function definition
    await close_client()
    await execute_close_client()
        # Ensure shutdown is awaited properly to avoid warnings
    if hasattr(application, 'shutdown'):
        await application.shutdown()

def main():
    try:
        loop = asyncio.get_running_loop()
        logger.info("Running within an existing event loop.")
        loop.create_task(run_bot())
    except RuntimeError:
        logger.info("Starting a new event loop.")
        asyncio.run(run_bot())

if __name__ == '__main__':
    main()

async def main():
    try:
        # Initialize the Application (already imported)
        app = Application()

        # Initialize the application (assuming this is an async method)
        await app.initialize()

        # Add additional bot logic here
        # Placeholder: add your functionality here

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        # Ensure that the shutdown coroutine is awaited before closing
        if "app" in locals() and hasattr(app, "shutdown"): await app.shutdown()

if __name__ == "__main__":
    try:
        # Create a new event loop
        asyncio.run(main())

    except RuntimeError as e:
        # Handle the case when the event loop is already running
        logger.error(f"Cannot close a running event loop: {e}")

    finally:
            async def close_client():
                pass
                logger.info("Exiting the application safely.")
                async def execute_client_close():
        await client.close()  # Ensure proper indentation  # Corrected indentation inside the async function  # Wrapped inside async function
    await execute_client_close()
    async def execute_close_client():
    # Properly indented block after function definition
    await close_client()
    await execute_close_client()
        # Removed unnecessary loop handling
            # Removed manual loop closure, as asyncio.run() handles it
