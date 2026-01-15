import asyncio
from websocket_client import BinanceCryptoClient


async def main():
    """Main function demonstrating WebSocket usage."""
    print("WebSocket Fintech Lab Demo")
    print("=" * 50)
    
    def handle_trade(message: dict):
        """Handle incoming trade data."""
        if 'p' in message:
            price = float(message['p'])
            quantity = float(message['q'])
            symbol = message['s']
            print(f"Trade: {symbol} - Price: ${price:,.2f}, Qty: {quantity:.6f}")
    
    # Create WebSocket client for Bitcoin
    client = BinanceCryptoClient("btcusdt")
    
    try:
        print("\nConnecting to Binance WebSocket...")
        print("Monitoring BTC/USDT trades (Press Ctrl+C to stop)\n")
        await client.listen(handle_trade, duration=60)
    except KeyboardInterrupt:
        print("\n\nStopping...")
        await client.disconnect()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
