# Python FinTech Lab - WebSocket Implementation

This project demonstrates WebSocket connectivity for real-time financial data streaming.

## Features

- **WebSocket Client**: Generic WebSocket client implementation
- **Binance Crypto Client**: Specialized client for cryptocurrency price streaming
- **Real-time Data**: Live trade data from Binance cryptocurrency exchange

## Installation

Install dependencies using `uv` or `pip`:

```bash
# Using uv (recommended)
uv pip install -e .

# Or using pip
pip install -e .
```

## Usage

### Running the Demo

The main demo connects to Binance WebSocket and displays real-time Bitcoin trades:

```bash
python main.py
```

### Using the WebSocket Client

```python
import asyncio
from websocket_client import BinanceCryptoClient

async def main():
    def handle_trade(message):
        print(f"Price: {message['p']}")
    
    client = BinanceCryptoClient("btcusdt")
    await client.listen(handle_trade, duration=30)

asyncio.run(main())
```

### Custom WebSocket Connection

```python
from websocket_client import WebSocketClient

async def custom_connection():
    client = WebSocketClient("wss://your-websocket-url")
    await client.connect()
    
    # Send message
    await client.send_message({"action": "subscribe"})
    
    # Receive message
    message = await client.receive_message()
    print(message)
    
    await client.disconnect()
```

## WebSocket Client API

### `WebSocketClient`

Base class for WebSocket connections.

**Methods:**
- `connect()`: Establish connection
- `disconnect()`: Close connection
- `send_message(message: dict)`: Send JSON message
- `receive_message()`: Receive and parse JSON message
- `listen(callback, duration)`: Listen for messages with callback

### `BinanceCryptoClient`

Specialized client for Binance cryptocurrency streams.

**Example symbols:**
- `btcusdt` - Bitcoin/USDT
- `ethusdt` - Ethereum/USDT
- `bnbusdt` - Binance Coin/USDT

## Requirements

- Python >= 3.13
- websockets >= 12.0

## Project Structure

```
.
├── main.py              # Main demo application
├── websocket_client.py  # WebSocket client implementation
├── pyproject.toml       # Project dependencies
└── README.md           # This file
```
