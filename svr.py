import asyncio
import websockets

# Set untuk menyimpan klien yang terhubung
connected_clients = set()

async def handle_client(websocket, path):
    # Tambahkan klien ke set saat terhubung
    connected_clients.add(websocket)
    print(f"Client connected: {websocket.remote_address}")

    try:
        async for message in websocket:
            print(f"Message from {websocket.remote_address}: {message}")
            # Kirim kembali pesan ke semua klien yang terhubung
            await asyncio.wait([client.send(f"Echo: {message}") for client in connected_clients])
    except websockets.exceptions.ConnectionClosed:
        print(f"Client disconnected: {websocket.remote_address}")
    finally:
        # Hapus klien dari set saat terputus
        connected_clients.remove(websocket)

async def main():
    # Mulai server WebSocket
    async with websockets.serve(handle_client, "localhost", 8020):
        print("WebSocket server started on ws://localhost:8020")
        await asyncio.Future()  # Menunggu selamanya

if __name__ == "__main__":
    asyncio.run(main())
