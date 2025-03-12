import asyncio
import websockets
import signal
import sys

# Flag untuk mengontrol loop client
running = True

def signal_handler(sig, frame):
    global running
    print("Menutup koneksi dengan baik...")
    running = False

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def ws_client():
    uri = "ws://localhost:8020/ws"
    reconnect_delay = 5  # Delay sebelum reconnect (dalam detik)
    
    while running:
        try:
            async with websockets.connect(uri) as websocket:
                print("Connected to WebSocket server")
                
                # Kirim pesan awal ke server
                await websocket.send("Hello from client")
                
                # Loop untuk menerima pesan
                while running:
                    try:
                        # Set timeout untuk menerima pesan
                        message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                        print(f"Message from server: {message}")
                    except asyncio.TimeoutError:
                        # Tidak ada pesan dalam waktu timeout
                        if not running:
                            break
                        # Kirim ping untuk menjaga koneksi tetap hidup
                        print("Sending ping to keep connection alive...")
                        await websocket.ping()
                    except websockets.exceptions.ConnectionClosed as e:
                        print(f"Connection closed: {e}")
                        # Tunggu sebelum reconnect
                        await asyncio.sleep(reconnect_delay)
                        break
        except Exception as e:
            if not running:
                break
            print(f"Failed to connect: {e}. Retrying in {reconnect_delay} seconds...")
            await asyncio.sleep(reconnect_delay)

if __name__ == "__main__":
    try:
        asyncio.run(ws_client())
    except KeyboardInterrupt:
        print("Client stopped by user")