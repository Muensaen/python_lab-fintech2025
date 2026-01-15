"""
Unit tests for WebSocket client implementation.
These tests use mocking to avoid requiring actual network connectivity.
"""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from websocket_client import WebSocketClient, BinanceCryptoClient


class TestWebSocketClient(unittest.TestCase):
    """Test cases for WebSocketClient."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.uri = "wss://test.example.com/ws"
        self.client = WebSocketClient(self.uri)
    
    def test_initialization(self):
        """Test client initialization."""
        self.assertEqual(self.client.uri, self.uri)
        self.assertIsNone(self.client.websocket)
        self.assertFalse(self.client.running)
    
    @patch('websockets.connect')
    def test_connect(self, mock_connect):
        """Test WebSocket connection."""
        mock_ws = AsyncMock()
        
        # Make connect return a coroutine that resolves to mock_ws
        async def mock_connect_coro(uri):
            return mock_ws
        
        mock_connect.side_effect = mock_connect_coro
        
        async def run_test():
            await self.client.connect()
            self.assertTrue(self.client.running)
            self.assertEqual(self.client.websocket, mock_ws)
        
        asyncio.run(run_test())
    
    def test_disconnect(self):
        """Test WebSocket disconnection."""
        mock_ws = AsyncMock()
        self.client.websocket = mock_ws
        self.client.running = True
        
        async def run_test():
            await self.client.disconnect()
            self.assertFalse(self.client.running)
            mock_ws.close.assert_called_once()
        
        asyncio.run(run_test())
    
    def test_send_message(self):
        """Test sending a message."""
        mock_ws = AsyncMock()
        self.client.websocket = mock_ws
        
        test_message = {"action": "subscribe", "symbol": "BTCUSDT"}
        
        async def run_test():
            await self.client.send_message(test_message)
            mock_ws.send.assert_called_once_with(json.dumps(test_message))
        
        asyncio.run(run_test())
    
    def test_receive_message(self):
        """Test receiving a message."""
        mock_ws = AsyncMock()
        test_data = {"price": "50000", "symbol": "BTCUSDT"}
        mock_ws.recv.return_value = json.dumps(test_data)
        self.client.websocket = mock_ws
        
        async def run_test():
            message = await self.client.receive_message()
            self.assertEqual(message, test_data)
            mock_ws.recv.assert_called_once()
        
        asyncio.run(run_test())
    
    @patch('websockets.connect')
    def test_listen_with_callback(self, mock_connect):
        """Test listening for messages with a callback."""
        mock_ws = AsyncMock()
        
        # Make connect return a coroutine that resolves to mock_ws
        async def mock_connect_coro(uri):
            return mock_ws
        
        mock_connect.side_effect = mock_connect_coro
        
        # Simulate receiving two messages then closing
        messages = [
            json.dumps({"price": "50000"}),
            json.dumps({"price": "51000"}),
        ]
        mock_ws.recv.side_effect = messages
        
        received_messages = []
        
        def callback(message):
            received_messages.append(message)
            if len(received_messages) >= 2:
                self.client.running = False
        
        async def run_test():
            await self.client.listen(callback, duration=1)
            self.assertEqual(len(received_messages), 2)
            self.assertEqual(received_messages[0]["price"], "50000")
            self.assertEqual(received_messages[1]["price"], "51000")
        
        asyncio.run(run_test())


class TestBinanceCryptoClient(unittest.TestCase):
    """Test cases for BinanceCryptoClient."""
    
    def test_initialization(self):
        """Test Binance client initialization."""
        symbol = "btcusdt"
        client = BinanceCryptoClient(symbol)
        
        expected_uri = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
        self.assertEqual(client.uri, expected_uri)
        self.assertEqual(client.symbol, symbol)
    
    def test_initialization_custom_symbol(self):
        """Test Binance client with custom symbol."""
        symbol = "ethusdt"
        client = BinanceCryptoClient(symbol)
        
        expected_uri = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
        self.assertEqual(client.uri, expected_uri)
        self.assertEqual(client.symbol, symbol)


class TestWebSocketClientIntegration(unittest.TestCase):
    """Integration tests for WebSocket client."""
    
    @patch('websockets.connect')
    def test_full_workflow(self, mock_connect):
        """Test complete connect-send-receive-disconnect workflow."""
        mock_ws = AsyncMock()
        
        # Make connect return a coroutine that resolves to mock_ws
        async def mock_connect_coro(uri):
            return mock_ws
        
        mock_connect.side_effect = mock_connect_coro
        
        test_message = {"action": "subscribe"}
        response_message = {"status": "subscribed"}
        mock_ws.recv.return_value = json.dumps(response_message)
        
        async def run_test():
            client = WebSocketClient("wss://test.example.com/ws")
            
            # Connect
            await client.connect()
            self.assertTrue(client.running)
            
            # Send message
            await client.send_message(test_message)
            mock_ws.send.assert_called_once()
            
            # Receive message
            message = await client.receive_message()
            self.assertEqual(message, response_message)
            
            # Disconnect
            await client.disconnect()
            self.assertFalse(client.running)
            mock_ws.close.assert_called_once()
        
        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
