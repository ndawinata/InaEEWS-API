import asyncio
import websockets

async def ws_client():
    # uri = "ws://localhost:8020/ws"
    uri = "ws://localhost:7001/ws_data"
    
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                print("Connected to WebSocket server")

                while True:
                    try:
                        message = await websocket.recv()
                        if message:
                            print(f"Message received: {message}")
                    except websockets.exceptions.ConnectionClosedError:
                        print("Connection closed unexpectedly. Reconnecting...")
                        break  # Keluar dari loop dalam untuk reconnect

        except Exception as e:
            print(f"Failed to connect: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)  # Tunggu 5 detik sebelum reconnect

asyncio.run(ws_client())