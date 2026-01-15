import asyncio
import json
import websockets
from websockets.asyncio.client import ClientConnection
from typing import Callable, Optional


class WebSocketClient:
    """
    A WebSocket client for connecting to financial data streams.
    Example: Connect to Binance cryptocurrency price feeds.
    """
    
    def __init__(self, uri: str):
        """
        Initialize the WebSocket client.
        
        Args:
            uri: The WebSocket URI to connect to
        """
        self.uri = uri
        self.websocket: Optional[ClientConnection] = None
        self.running = False
    
    async def connect(self):
        """Establish WebSocket connection."""
        try:
            self.websocket = await websockets.connect(self.uri)
            self.running = True
            print(f"Connected to {self.uri}")
        except Exception as e:
            print(f"Connection error: {e}")
            raise
    
    async def disconnect(self):
        """Close the WebSocket connection."""
        self.running = False
        if self.websocket:
            await self.websocket.close()
            print("Disconnected")
    
    async def send_message(self, message: dict):
        """
        Send a message through the WebSocket.
        
        Args:
            message: Dictionary to send as JSON
        """
        if self.websocket:
            await self.websocket.send(json.dumps(message))
    
    async def receive_message(self) -> Optional[dict]:
        """
        Receive a message from the WebSocket.
        
        Returns:
            Parsed JSON message as dictionary, or None if error
        """
        if self.websocket:
            try:
                message = await self.websocket.recv()
                return json.loads(message)
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed")
                self.running = False
                return None
            except Exception as e:
                print(f"Error receiving message: {e}")
                return None
        return None
    
    async def listen(self, callback: Callable[[dict], None], duration: Optional[int] = None):
        """
        Listen for messages and process them with a callback.
        
        Args:
            callback: Function to call with each received message
            duration: Optional duration in seconds to listen (None = indefinite)
        """
        if not self.websocket:
            await self.connect()
        
        start_time = asyncio.get_running_loop().time()
        
        while self.running:
            if duration and (asyncio.get_running_loop().time() - start_time) > duration:
                break
            
            message = await self.receive_message()
            if message:
                callback(message)
        
        await self.disconnect()


class BinanceCryptoClient(WebSocketClient):
    """WebSocket client specifically for Binance cryptocurrency streams."""
    
    def __init__(self, symbol: str = "btcusdt"):
        """
        Initialize Binance crypto client.
        
        Args:
            symbol: Trading pair symbol (e.g., 'btcusdt', 'ethusdt')
        """
        # Binance WebSocket stream for individual symbol ticker
        uri = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
        super().__init__(uri)
        self.symbol = symbol


async def example_crypto_price_monitor():
    """Example: Monitor Bitcoin price in real-time."""
    
    def price_callback(message: dict):
        """Process incoming price data."""
        if 'p' in message:  # 'p' is price in Binance trade stream
            price = message['p']
            symbol = message['s']
            quantity = message['q']
            print(f"{symbol}: Price={price}, Quantity={quantity}")
    
    # Create client for Bitcoin/USDT trading pair
    client = BinanceCryptoClient("btcusdt")
    
    print("Starting Bitcoin price monitor (will run for 30 seconds)...")
    try:
        # Listen for 30 seconds
        await client.listen(price_callback, duration=30)
    except KeyboardInterrupt:
        print("\nStopped by user")
        await client.disconnect()


if __name__ == "__main__":
    # Run the example
    asyncio.run(example_crypto_price_monitor())
