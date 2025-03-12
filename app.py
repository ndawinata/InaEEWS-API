from robyn import Robyn, WebSocket
from robyn.robyn import Response, Request, Headers
import xml.etree.ElementTree as ET
from SetDB import Database
import secrets
import logging
import json
# Initialize the Robyn app
app = Robyn(__file__)

INAEWS_DB = {
    'user':'postgres',
    'password':'r00tBMKG2023!',
    'database': 'inaeews',
    'host': 'localhost',
    'port': '5432'
}

SIMAP_DB = {
    'user':'postgres',
    'password':'r00tBMKG2023!',
    'database': 'simap',
    'host': 'localhost',
    'port': '5432'
}

db = Database(INAEWS_DB)
db_simap = Database(SIMAP_DB)

logging.basicConfig(level=logging.INFO)

# Set up WebSocket
websocket = WebSocket(app, "/ws")
connected_clients = {}  # Set to store connected WebSocket clients

async def cek_valid_token(token):
    st = await db.fetchone(f'''SELECT * FROM client WHERE token = '{token}' ''')
    if st:
        return True
    else:
        return False

# WebSocket event handlers
@websocket.on("connect")
async def notify_connect(ws):
    try:
        token = ws.query_params.get("token")

        if await cek_valid_token(token):
            connected_clients[ws.id] = ws
            await ws.async_send_to(ws.id, json.dumps({"success": True, "message": "Connected"}))
            return ""

        else:
            await ws.async_send_to(ws.id, json.dumps({"success": False, "message": "Invalid token"}))
            await ws.close()
            return ""

    except Exception as e:
        await ws.async_send_to(ws.id, json.dumps({"success": False, "message": "Invalid token"}))
        await ws.close()
        return ""

@websocket.on("message")
async def notify_message(ws, message):
   
    return f"Recv: {ws.id} {message}"

@websocket.on("close")
async def notify_close(ws):
    if ws.id in connected_clients:
        del connected_clients[ws.id]

# Endpoint to broadcast messages to all connected WebSocket clients

async def get_latest():
    st = await db.fetchone('SELECT * FROM eew_diseminasi ORDER BY ot DESC LIMIT 1')
    if st:
        # Use the correct field names based on the printed output
        record_dict = {
            "ot": st['ot'].strftime("%Y-%m-%d %H:%M:%S.%f"),  # Adjust if needed
            "mag": st['mag'],
            "lat": st['lat'],
            "lon": st['lon'],
            "depth": st['depth'],
            "area": st['area']
        }

        root = ET.Element("Earthquake")
        
        # Add child elements
        ET.SubElement(root, "ot").text = st['ot'].strftime("%Y-%m-%d %H:%M:%S.%f")  # Adjust if needed
        ET.SubElement(root, "mag").text = str(st['mag'])
        ET.SubElement(root, "lat").text = str(st['lat'])
        ET.SubElement(root, "lon").text = str(st['lon'])
        ET.SubElement(root, "depth").text = str(st['depth'])
        ET.SubElement(root, "area").text = st['area']

        return json.dumps(record_dict), ET.tostring(root, encoding='unicode')
    else:
        return None

async def get_event_history(n):
    st = await db.fetch(f'SELECT * FROM eew_diseminasi ORDER BY ot DESC LIMIT {n}')
    
    if st:
        dJson = json.dumps([
        {
            "ot": record['ot'].strftime("%Y-%m-%d %H:%M:%S.%f"),
            "mag": record['mag'],
            "lat": record['lat'],
            "lon": record['lon'],
            "depth": record['depth'],
            "area": record['area']
        }
        for record in st
        ])

        dXml = ET.Element("Earthquakes")
        for record in st:
            earthquake = ET.SubElement(dXml, "Earthquake")
            ET.SubElement(earthquake, "ot").text = record['ot'].strftime("%Y-%m-%d %H:%M:%S.%f")
            ET.SubElement(earthquake, "mag").text = str(record['mag'])
            ET.SubElement(earthquake, "lat").text = str(record['lat'])
            ET.SubElement(earthquake, "lon").text = str(record['lon'])
            ET.SubElement(earthquake, "depth").text = str(record['depth'])
            ET.SubElement(earthquake, "area").text = record['area']

        return dJson, ET.tostring(dXml, encoding='unicode')
    else:
        return None


@app.post("/posteew")
async def broadcast_message(request):
    try:
        data = request.json() 
        print(data)
        for client_id, client_ws in connected_clients.items():
            await client_ws.async_send_to(client_id, json.dumps(data))

        return Response(status_code=200, headers=Headers({}), description="OK")

    except Exception as e:
        return Response(status_code=406, headers=Headers({}), description="Not Acceptable")

@app.post("/register")
async def register_client(request: Request):
    try:
        form_data = request.form_data
        name = form_data.get("name")
        email = form_data.get("email")
        hp = form_data.get("hp")
        # 0 = public, 1 = internal, 2 = private, 3 = vip
        previlages = 0
        token = secrets.token_hex(16)

        st = await db.fetchone(f"SELECT id FROM client WHERE email = '{email}'")
        if st:
            client_id = await db.execute_return(f''' UPDATE client SET name = '{name}', hp = '{hp}', token = '{token}', previlages = '{previlages}', updated_at = CURRENT_TIMESTAMP WHERE email = '{email}' RETURNING id; ''')
        else:
            client_id = await db.execute_return(f''' INSERT INTO client (name, email, hp, token, previlages, created_at) VALUES ('{name}','{email}','{hp}','{token}','{previlages}', CURRENT_TIMESTAMP) RETURNING id; ''')
        st = await db.fetch_to_df(f"SELECT * FROM client WHERE id = '{client_id}'")
        djson = json.loads(st.to_json(orient='records', date_format='iso'))[0]
        return {"success": True, "data": djson}
    except Exception as e:
        print('error : ',e)
        return {"success": False, "token": None}   

@app.get("/json/latest")
async def latest_json():
    try:
        dataJson, _ = await get_latest()
        return Response(
            status_code=200,
            headers=Headers({"Content-Type": "application/json"}),
            description=dataJson
        )
    except Exception as e:
        return Response(
            status_code=500,
            headers=Headers({"Content-Type": "application/json"}),
            description=json.dumps({"success": False, "data": None})
        )

@app.get("/xml/latest")
async def latest_xml():
    try:
        _, dataXml = await get_latest()
        return Response(
            status_code=200,
            headers=Headers({"Content-Type": "application/xml"}),
            description=dataXml
        )
    except Exception as e:
        return Response(
            status_code=500,
            headers=Headers({"Content-Type": "application/xml"}),
            description="<error>Unable to fetch data</error>"
        )

@app.get("/json/last-15")
async def get15_json():
    try:
        dataJson, _ = await get_event_history(15)
        return Response(
            status_code=200,
            headers=Headers({"Content-Type": "application/json"}),
            description=dataJson
        )
    except Exception as e:
        return Response(
            status_code=500,
            headers=Headers({"Content-Type": "application/json"}),
            description=json.dumps({"success": False, "data": None})
        )

@app.get("/xml/last-15")
async def get15_xml():
    try:
        _ , dataXml = await get_event_history(15)
        return Response(
            status_code=200,
            headers=Headers({"Content-Type": "application/xml"}),
            description=dataXml
        )
    except Exception as e:
        return Response(
            status_code=500,
            headers=Headers({"Content-Type": "application/xml"}),
            description=dataXml
        )

@app.get("/json/last-30")
async def get30_json():
    try:
        dataJson, _ = await get_event_history(30)
        return Response(
            status_code=200,
            headers=Headers({"Content-Type": "application/json"}),
            description=dataJson
        )
    except Exception as e:
        return Response(
            status_code=500,
            headers=Headers({"Content-Type": "application/json"}),
            description=json.dumps({"success": False, "data": None})
        )

@app.get("/xml/last-30")
async def get30_xml():
    try:
        _ , dataXml = await get_event_history(30)
        return Response(
            status_code=200,
            headers=Headers({"Content-Type": "application/xml"}),
            description=dataXml
        )
    except Exception as e:
        return Response(
            status_code=500,
            headers=Headers({"Content-Type": "application/xml"}),
            description=dataXml
        )



# Start the Robyn app
if __name__ == "__main__":
    app.start(host="0.0.0.0", port=8020)
