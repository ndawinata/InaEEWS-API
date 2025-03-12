from fastapi import FastAPI, Request, WebSocket, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import json 
from datetime import datetime, timedelta, timezone
from geopy.distance import geodesic
import pytz
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
from SetDB import Database
from typing import List
from starlette.websockets import WebSocketDisconnect
from typing import Set
import uvicorn
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi.responses import FileResponse, JSONResponse
from user_agents import parse

print('SimapV2 Start')

tz = pytz.timezone('Asia/Jakarta')
utc = pytz.UTC

app = FastAPI()

# Konfigurasi untuk dua database berbeda
SIMAP_DB = {
    'user':'postgres',
    'password':'r00tBMKG2023!',
    'database': 'simap',
    'host': 'localhost',
    'port': '5432'
}

db = Database(SIMAP_DB)

# CORS Configuration for HTTP and WebSocket
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

base_dir = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(base_dir, "static")
template_dir = os.path.join(base_dir, "templates")

event_directory = os.path.join(base_dir, "static/json/event")

templates = Jinja2Templates(directory=template_dir)
app.mount("/static", StaticFiles(directory=static_dir), name="static")
app.mount("/simapEvent", StaticFiles(directory=event_directory), name="simapev")

domain = "https://simap.bmkg.go.id"

# scheduler = BackgroundScheduler()
scheduler = AsyncIOScheduler()
scheduler.start()

async def startup():
    print("Application startup")
    # Logika untuk membuat pool, misalnya:
    await db.create_pool()

async def shutdown():
    print("Application shutdown")
    # Logika untuk menutup pool, misalnya:
    await db.close_pool()

app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)

# ------------- Fungsi tanpa route --------------


def hitung_otP(row, koordinat_A, OT):
    if row['lat'] and row['lon']:
        koordinat_B = (float(row['lat']), float(row['lon']))
        jarak = geodesic(koordinat_A, koordinat_B).kilometers
        otP = OT + (jarak / 8)
        return otP
    return None

def hitung_jarak1(row, koordinat_A):
    koordinat_B = (row['lat'], row['lon'])
    
    return geodesic(koordinat_A, koordinat_B).kilometers

def hitung_jarak(row, koordinat_A):
    koordinat_B = (row['epicenterLat'], row['epicenterLon'])
    return geodesic(koordinat_A, koordinat_B).kilometers

async def find_listPga(id):
    df = await db.fetch_to_df(f'''
                              SELECT l.url, l.kode, l.pga, l.pgv, l.pgd, l.lat, l.lon, l.timestamp, l.kab, l."history_id", k.nama 
                                FROM "listPga" l
                                LEFT JOIN "kota" k ON l.kode = k."Kode"
                                WHERE l."history_id" = '{id}';
                              ''')
    if df.empty:
        return []
    
    dat = json.loads(df.to_json(orient='records', date_format='iso'))
    return dat

async def find_pga(req):
    # print(req)
    # OT = utc.localize(datetime.strptime(req["ot"], '%Y-%m-%dT%H:%M:%S.%f'))
    # OT = utc.localize(datetime.strptime(req["ot"], '%Y-%m-%d %H:%M:%S'))
    OT = datetime.strptime(req["ot"], '%Y-%m-%d %H:%M:%S')
    koordinat_A = (req['epicLat'], req['epicLon'])
    # 1. Ambil Stasiun dengan 0 < timestamp - ot < 10 mnt
    df = await db.fetch_to_df(f"SELECT * FROM pga WHERE (timestamp-{OT.timestamp()}) BETWEEN 0 AND 600")
    if df.empty:
        return []
    # print(qq)
    # print(df)

    # 2. estimasi waktu P dengan rumus: t=ot+jarak/8
    df['otP'] = df.apply(hitung_otP, axis=1, args=(koordinat_A, OT.timestamp()))

    df_1 = df.dropna(subset=['otP'])
    df1 = df_1.copy()
    df1['timestamp'] = pd.to_numeric(df1['timestamp'], errors='coerce')
    # 3. Ambil stasiun yang -10 < timestamp-t < 20
    df2 = df1.query('-10 < timestamp - otP < 20')
    if df2.empty:
        return []
    
    df2 = df2.assign(abs_diff=df2.apply(lambda row: abs(row['timestamp'] - row['otP']), axis=1))
    
    # Buat salinan eksplisit dari DataFrame baru yang dikembalikan oleh fungsi apply()
    new_df = df2.apply(lambda row: abs(row['timestamp'] - row['otP']), axis=1)
    abs_diff_slice = new_df.copy()
    abs_diff_slice['abs_diff'] = abs_diff_slice
    df2 = df2.assign(abs_diff=abs_diff_slice['abs_diff'])
    
    df3 = df2.groupby('kode', group_keys=True).apply(lambda x: x[x['abs_diff'] == x['abs_diff'].min()])
    df3.drop(columns=['abs_diff','otP'])
    if df3.empty:
        return []
    df3 = df3.reset_index(level='kode', drop=True)
    
    # tambah filter, ambil PGA yang lokasi berada < 500 km
    df3['jarak'] = df3.apply(hitung_jarak1, axis=1, args=(koordinat_A,))
    if req['mag'] > 5:
        df3 = df3.query('jarak < 600')
    else:
        df3 = df3.query('jarak < 150')

    
    df3 = df3.loc[:, df3.columns != 'kab']
    uKode = df3['kode'].unique()
    df4 = await db.fetch_to_df(f''' SELECT "Kode" as kode, "KODE_KAB" as kab FROM kota WHERE "Kode" IN ('{"', '".join(map(str, uKode))}') ''')
    if df4.empty:
        return []
    df5 = pd.merge(df3, df4, on='kode', how="left")
    dat = json.loads(df5.to_json(orient='records', date_format='iso'))
    return dat

# def grabPgaHis_wrapper(id):
#     asyncio.create_task(grabPgaHis(id))

def grabPgaHis_wrapper(id, loop):
    asyncio.run_coroutine_threadsafe(grabPgaHis(id), loop)

async def jadwalin(id):
    # now = datetime.now(tz)
    now = datetime.now()

    # Cek apakah sudah ada job dengan ID yang sama
    existing_jobs = scheduler.get_jobs()
    for job in existing_jobs:
        if job.args == (id,):
            print(f'Job dengan ID {id} sudah ada. Tidak menambahkan job baru.')
            return

    try:
        initTimeSchedule = now + timedelta(seconds=10)
        loop = asyncio.get_event_loop()
        scheduler.add_job(func=grabPgaHis_wrapper, args=[id, loop], trigger='date', run_date=initTimeSchedule)
        # scheduler.add_job(func=grabPgaHis, args=[id], trigger='date', run_date=initTimeSchedule.strftime("%Y-%m-%d %H:%M:%S"))
        timeSchedule = now
        for i in range(4): 
            timeSchedule = timeSchedule + timedelta(seconds=30)
                    
            scheduler.add_job(func=grabPgaHis_wrapper, args=[id, loop], trigger='date', run_date=timeSchedule)
            # scheduler.add_job(func=grabPgaHis, args=[id], trigger='date', run_date=timeSchedule.strftime("%Y-%m-%d %H:%M:%S"))
            print(f'ini Schedule Event dg History id {id} {timeSchedule.strftime("%Y-%m-%d %H:%M:%S")}')
    except Exception as e:
        print(f'Gagal Schedule Event dg History id {id} ', e)

async def saveEvent(data, jns):
    if jns == 'pgn':
        # jika ada post event dari pgn
        try:
            ot = datetime.strptime(data["ot"], '%Y-%m-%d %H:%M:%S.%f')
        except:
            ot = datetime.strptime(data["ot"], '%Y-%m-%d %H:%M:%S')
        

        eventPgn_id = data['eventPgn_id']
        lower_ot = ot - timedelta(minutes=2)
        upper_ot = ot + timedelta(minutes=2)
        koordinat_A = (data["epicLat"], data["epicLon"])

        # cek apakah id ada di tabel PGN ?
        st = await db.fetchone(f'''SELECT * FROM event_pgn WHERE "eventPgn_id"='{eventPgn_id}' ''')

        # jika Ada
        if st:
            # update data di tabel PGN
            await db.execute(f''' UPDATE "event_pgn" SET ot = '{ot}', mag = {data['mag']}, area = '{data["area"]}', "epicLon" = {data['epicLon']}, "epicLat" = {data['epicLat']}, depth = {data['depth']} WHERE "eventPgn_id" = '{eventPgn_id}' ''')
        # tidak ada
        else:
            # cek di tabel EEW +- 1.2 dan jarak 200km
            await db.execute(f''' INSERT INTO "event_pgn" ("eventPgn_id", "dTime", ot , mag , area, "epicLon", "epicLat", depth ) VALUES ('{eventPgn_id}', '{data["dTime"]}', '{ot}', {data["mag"]}, '{data["area"]}', {data["epicLon"]}, {data["epicLat"]}, {data["depth"]} ) ''')
            # cek di tabel EEW +- 1.2 m
            print('df')
            df = await db.fetch_to_df(f''' SELECT * FROM event_eews WHERE "originTime" > '{lower_ot}' AND "originTime" < '{upper_ot}' ''')
            # cek apakah ada event eew yang tercatat
            if df.empty:
                # jika tidak ada, buat row history baru
                hist_id = await db.execute_return(f''' INSERT INTO history (ot, pgn_id) VALUES ('{ot}','{eventPgn_id}') RETURNING history_id; ''')
                print('hist_id : ',hist_id)
                await jadwalin(hist_id)
            else:
                df['jarak'] = df.apply(hitung_jarak, axis=1, args=(koordinat_A,))
                df_terdekat = df[df['jarak'] <= 150]
                tdkt = df_terdekat[df_terdekat['jarak'] == df_terdekat['jarak'].min()]
                if tdkt.empty:
                    # jika tidak ada, buat row history baru
                    hist_id = await db.execute_return(f''' INSERT INTO history (ot, pgn_id) VALUES ('{ot}','{eventPgn_id}') RETURNING history_id; ''')
                    print('hist_id1 : ',hist_id)
                    
                    await jadwalin(hist_id)
                else:
                    # jika ada update history masukkan eew_id
                    id_eew = tdkt.iloc[0]['identifier']
                    h = await db.fetch_to_df(f''' SELECT * FROM history WHERE eew_id = '{id_eew}' ''')

                    if h.empty:
                        # jika tidak ada history, buat baru dan masukkan id eew dan id pgn
                        hist_id = await db.execute_return(f''' INSERT INTO history (ot, pgn_id, eew_id) VALUES ('{ot}','{eventPgn_id}', '{id_eew}') RETURNING history_id; ''')
                        print('hist_id2 : ',hist_id)
                        
                        await jadwalin(hist_id)
                    else:
                        # jika ada history, update eew_id 
                        hist_id = h.iloc[0]['history_id']
                        print('hist_id3 : ',hist_id)

                        await db.execute(f''' UPDATE history SET ot = '{ot}', pgn_id = '{eventPgn_id}', eew_id = '{id_eew}' WHERE history_id = {hist_id} ''')
    
    else:
        # jika ada post event dari eew
        ot = datetime.strptime(data["originTime"], '%Y-%m-%d %H:%M:%S')
        id_eew = data['identifier']
        lower_ot = ot - timedelta(minutes=2)
        upper_ot = ot + timedelta(minutes=2)
        koordinat_A = (data["epicenterLat"], data["epicenterLon"])
        # cek apakah id ada di tabel EEWS ?
        st = await db.fetchone(f'''SELECT * FROM event_eews WHERE identifier = '{id_eew}' ''')
        # jika Ada
        if st:
        #     # update data di tabel EEWS
            await db.execute(f''' UPDATE event_eews SET "originTime" = '{ot}', sent = '{data["sent"]}', "epicenterLat" = {data["epicenterLat"]}, "epicenterLon" = {data["epicenterLon"]}, depth = {data['depth']}, magnitude = {data['magnitude']}, "senderName" = '{data["senderName"]}', "references" = '{data["references"]}' WHERE identifier = '{id_eew}' ''')
        # tidak ada
        else:
            await db.execute(f'''INSERT INTO "event_eews" ("dTime", "originTime", sent, "epicenterLat", "epicenterLon", depth, magnitude, identifier, "senderName", "references" ) VALUES ('{data["dTime"]}', '{ot}', '{data["sent"]}', {data["epicenterLat"]}, {data["epicenterLon"]}, {data['depth']}, {data['magnitude']}, '{id_eew}', '{data["senderName"]}', '{data["references"]}') ''')
            hist_id = await db.execute_return(f'''INSERT INTO history (ot, eew_id) VALUES ('{ot}','{id_eew}') RETURNING history_id; ''')
            print('hist_id5 : ',hist_id)
            
            await jadwalin(hist_id)

async def saveFeed(data):
    try:
        await db.execute(f''' INSERT INTO "feedback" ("dTime", "recvTime" , "lat", "lon", "device", "identifier") VALUES ('{data["dTime"]}', '{data["receiveTime"]}', {data["lat"]}, {data["lon"]}, '{data["device"]}', '{data["identifier"]}' ) ''')
    except Exception as e:
        print('gagal save : ', e)


# ------------- Route --------------

# @app.get("/list")
# async def list_simap_event():
#     try:
#         files = os.listdir(event_directory)
#         file_links = [f'<li><a href="/simapEvent/{file}">{file}</a></li>' for file in files]
#         file_list_html = "<ul>" + "".join(file_links) + "</ul>"
#         return f"""
#         <html>
#         <head>
#             <title>Daftar File</title>
#         </head>
#         <body>
#             <h1>Daftar File di /simapEvent/</h1>
#             {file_list_html}
#         </body>
#         </html>
#         """
#     except FileNotFoundError:
#         raise HTTPException(status_code=404, detail="Directory not found")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

@app.get("/simapEvent/{filename}")
async def get_simap_event_file(filename: str):
    file_path = os.path.join(event_directory, filename)
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=filename)
    else:
        raise HTTPException(status_code=404, detail="File not found")


# @app.get("/")
# async def index(request: Request):
#     user_agent = request.headers.get('user-agent')
#     user_agent_parsed = parse(user_agent)
    
#     if user_agent_parsed.is_mobile:
#         return templates.TemplateResponse("utama-mobile.html", {"request": request})
#     else:
#         return templates.TemplateResponse("index2.html", {'request':request})

@app.get("/")
async def index(request: Request):
    user_agent = request.headers.get('user-agent')

    if user_agent:
        user_agent_parsed = parse(user_agent)
        if user_agent_parsed.is_mobile:
            # Jika mobile, tampilkan template mobile
            return templates.TemplateResponse("utama-mobile.html", {"request": request})
        else:
            # Jika bukan mobile, tampilkan template desktop
            return templates.TemplateResponse("index2.html", {'request': request})
    else:
        # Jika user-agent tidak ditemukan, fallback ke template desktop
        return templates.TemplateResponse("index2.html", {'request': request})
    
@app.get("/region")
async def index(request: Request):
    return templates.TemplateResponse("index1.html", {'request':request})

@app.get("/detail/{id}")
async def detail(request: Request, id: int):
    data = await histid(id)
    data['feed'] = None
    if data['eew'] :
        
        df = await db.fetch_to_df(f'''SELECT * FROM feedback WHERE identifier='{ data["eew"]["identifier"] }' ''') 
        json_feed = json.loads(df.to_json(orient='records', date_format='iso'))
        data['feed'] = json_feed

    data['ot'] = data['ot'].strftime('%Y-%m-%d %H:%M:%S')
    return templates.TemplateResponse("detail.html", {'request':request, 'data':data, 'base_url':domain})

@app.get("/lastevent")
async def lastevent(request: Request):
    
    return templates.TemplateResponse("lastevent.html", {'request':request})

@app.get('/history/{id}')
async def histid(id:str):
    df = await db.fetch_to_df(f'SELECT * FROM history WHERE history_id={id}')
    pgn_id = df.iloc[0]['pgn_id']
    eew_id = df.iloc[0]['eew_id']
    ot = df.iloc[0]['ot']

    df_pgn = pd.DataFrame()
    df_eew = pd.DataFrame()
    json_pgn = None
    json_eew = None
    mag = None
    epic = None

    if eew_id:    
        df_eew = await db.fetch_to_df(f'''SELECT * FROM event_eews WHERE "identifier" = '{eew_id}' ''')
        json_eew = json.loads(df_eew.to_json(orient='records', date_format='iso'))
        mag = float(df_eew.iloc[0]['magnitude'])
        epic = (float(df_eew.iloc[0]['epicenterLat']), float(df_eew.iloc[0]['epicenterLon']))

    if pgn_id:
        df_pgn = await db.fetch_to_df(f'''SELECT * FROM event_pgn WHERE "eventPgn_id" = '{pgn_id}' ''')
        json_pgn = json.loads(df_pgn.to_json(orient='records', date_format='iso'))
        mag = float(df_pgn.iloc[0]['mag'])
        epic = (float(df_pgn.iloc[0]['epicLat']), float(df_pgn.iloc[0]['epicLon']))


    dd = {
        "eew_id": eew_id,
        "history_id": id,
        "ot": ot,
        "pgn_id": pgn_id,
        "pgn": json_pgn[-1] if json_pgn else None,
        "eew": json_eew[-1] if json_eew else None
    }

    if dd['pgn']:
        datPGA = await find_listPga(id)
    else:
        modEEW = dd['eew']
        modEEW['ot'] = dd['eew']['originTime']
        modEEW['epicLat'] = dd['eew']['epicenterLat']
        modEEW['epicLon'] = dd['eew']['epicenterLat']
        datPGA = await find_listPga(id)
    
    # # print(datPGA) 
    filtered_datapga = []
    for site in datPGA:
        distance = geodesic(epic, (site['lat'], site['lon'])).kilometers

        if mag < 5:
            if distance <= 150:  
                filtered_datapga.append(site)
        else:
            if distance <= 600:  
                filtered_datapga.append(site)
        
    dd['datapga'] = filtered_datapga

    return dd

@app.get('/playback/{id}')
async def playback(request: Request, id: int):
    dfH = await db.fetch_to_df(f'SELECT * FROM history WHERE history_id={id}')
    eventPgn_id = dfH.iloc[0]["pgn_id"]
    eew_id = dfH.iloc[0]["eew_id"]
    ot = dfH.iloc[0]['ot']

    dfP = dfE = pgn = eew = pd.DataFrame()
    firstTS = lastTS = None

    if eventPgn_id:
        dfP = await db.fetch_to_df(f'''SELECT * FROM event_pgn WHERE "eventPgn_id"='{eventPgn_id}' ''')
        dfP['dTime'] = pd.to_datetime(dfP['dTime'])
        pgn = json.loads(dfP.to_json(orient='records', date_format='epoch', date_unit='s'))[0]
    
    if eew_id:
        dfE = await db.fetch_to_df(f'''SELECT * FROM event_eews WHERE identifier ='{eew_id}' ''')
        dfE['dTime'] = pd.to_datetime(dfE['dTime'])
        eew = json.loads(dfE.to_json(orient='records', date_format='epoch', date_unit='s'))[0]
    
    dflP = await db.fetch_to_df(f'SELECT * FROM "listPga" WHERE history_id={id} ORDER BY timestamp DESC')
    dflP['timestamp'] = dflP['timestamp'].astype(float).round()
    # lsKode = dflP['kode'].str.strip().drop_duplicates().tolist()

    pga = json.loads(dflP.to_json(orient='records', date_format='epoch', date_unit='s'))
    dictPga = {int(item['timestamp']): {k: v for k, v in item.items() if k != 'timestamp'} for item in pga}

    lastTS = list(dictPga.keys())[0]

    if eventPgn_id and eew_id:
        firstTS = pgn['ot']
        if pgn['ot'] + 120 > lastTS:
            lastTS = pgn['ot'] + 120
    elif eventPgn_id:
        firstTS = pgn['ot']
        if pgn['ot'] + 120 > lastTS:
            lastTS = pgn['ot'] + 120
    else:
        firstTS = eew['originTime']
        if eew['dTime'] > lastTS:
            lastTS = eew['dTime']

    data = {
        "pgn":None if dfP.empty else pgn,
        "eew":None if dfE.empty else eew,
        "pga":dictPga,
        "firstTS": firstTS,
        "lastTS": lastTS
    }

    data['feed'] = None
    if data['eew'] :
        
        df_feed = await db.fetch_to_df(f'''SELECT * FROM feedback WHERE identifier='{ data["eew"]["identifier"] }' ''') 
        json_feed = json.loads(df_feed.to_json(orient='records', date_format='epoch', date_unit='s'))
        dictFeed = {}
        for item in json_feed:
            recv_time = item['recvTime']
            
            # Cek apakah recvTime sudah ada dalam dictionary
            if recv_time in dictFeed:
                # Jika recvTime sudah ada, tambahkan item ke dalam list
                dictFeed[recv_time].append(item)
            else:
                # Jika recvTime belum ada, buat list baru dengan item tersebut
                dictFeed[recv_time] = [item]
            
        data['feed'] = dictFeed

    
    return templates.TemplateResponse("playback.html", {'request':request,'data':data})

@app.post("/postpgn")
async def post_pgn(data: dict):
    data['dTime'] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    dat = {
            "err":None,
            "data":data,
            "type":"pgn"
    }
    print(data)
    try:
        await saveEvent(data, 'pgn')
        dat['success'] = True
    except Exception as e:
        # print(e)
        dat['success'] = False
        dat['err'] = str(e.args)

    for connection in connections1:
        await connection.send_json(dat)

    return dat

@app.post("/posteew")
async def post_eew(data: dict):
    data['dTime'] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    dat = {
            "err":None,
            "data":data,
            "type":"eew"
    }
    print(data)
    try:
        await saveEvent(data, 'eew')
        dat['success'] = True
    except Exception as e:
        # print(e)
        dat['success'] = False
        dat['err'] = str(e.args)

    for connection in connections1:
        await connection.send_json(dat)

    return dat

@app.post("/posteew-test")
async def post_eew_test(data: dict):
    data['dTime'] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    dat = {
            "err":None,
            "data":data,
            "type":"eew-test"
    }
    print(data)
    try:
        # await saveEvent(data, 'eew')
        dat['success'] = True
    except Exception as e:
        # print(e)
        dat['success'] = False
        dat['err'] = str(e.args)

    for connection in connections1:
        await connection.send_json(dat)

    return dat

@app.post("/postpgn-test")
async def post_pgn_test(data: dict):
    data['dTime'] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    dat = {
            "err":None,
            "data":data,
            "type":"pgn-test"
    }
    print(data)
    try:
        # await saveEvent(data, 'pgn')
        dat['success'] = True
    except Exception as e:
        # print(e)
        dat['success'] = False
        dat['err'] = str(e.args)

    for connection in connections1:
        await connection.send_json(dat)

    return dat

@app.post("/feedback")
async def post_feed(data: dict):
    data['dTime'] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    
    dat = {
            "err":None,
            "data":data,
            "type":"feed"
    }
    try:
        await saveFeed(data)
        dat['success'] = True
    except Exception as e:
        # print(e)
        dat['success'] = False
        dat['err'] = str(e.args)

    for connection in connections1:
        await connection.send_json(dat)

    return dat

@app.get('/grab/{id}')
async def grabPgaHis(id: int):
    print('lagi grab nih -----')
    df = await db.fetch_to_df(f'SELECT * FROM history WHERE history_id={id}')
    pgn_id = df.iloc[0]['pgn_id']
    eew_id = df.iloc[0]['eew_id']
    ot = df.iloc[0]['ot'] 

    dat = {
        "ot":ot.strftime('%Y-%m-%d %H:%M:%S'),
        "epicLat":None,
        "epicLon":None,
        "mag":None
    }

    df_pgn = await db.fetch_to_df(f'''SELECT * FROM event_pgn WHERE "eventPgn_id" = '{pgn_id}' ''')
    df_eew = await db.fetch_to_df(f'''SELECT * FROM event_eews WHERE "identifier" = '{eew_id}' ''')

    if not df_eew.empty:
        dat['mag'] = float(df_eew.iloc[0]['magnitude'])

    if not df_pgn.empty:
        dat['mag'] = float(df_pgn.iloc[0]['mag'])

    if not df_pgn.empty:
        dat['epicLat'] = float(df_pgn.iloc[0]['epicLat'])
        dat['epicLon'] = float(df_pgn.iloc[0]['epicLon'])
    else:
        dat['epicLat'] = float(df_eew.iloc[0]['epicenterLat'])
        dat['epicLon'] = float(df_eew.iloc[0]['epicenterLon'])
    
    # print('dat : ', dat)

    try:
        datapga  = await find_pga(dat)
        if len(datapga) == 0:
            datapga = []
        # print('k : ',datapga)
    except Exception as e:
        print('errrr ' , e)
        datapga = []

    # print(datapga)
    if datapga:
        for pga in datapga:
            # print('ada pga')
            try:
                query = f'''
                    INSERT INTO "listPga" (url, kode, pga, pgv, pgd, lat, lon, timestamp, kab, history_id)
                    VALUES ('{pga["url"]}', '{pga["kode"]}', {pga["pga"]}, {pga["pgv"]}, {pga["pgd"]}, {pga["lat"]}, {pga["lon"]}, {pga["timestamp"]}, '{pga["kab"]}', {id})
                    ON CONFLICT (url)
                    DO UPDATE SET pga = EXCLUDED.pga, pgv = EXCLUDED.pgv, pgd = EXCLUDED.pgd, timestamp = EXCLUDED.timestamp
                '''
                await db.execute(query)
            except Exception as e:
                print('error 1: ', e)
        # kirim ke klien kalau ada capture pga baru
        for connection in connections1:
            await connection.send_json({"type":"new"})

    return datapga
    # return 'aman'

@app.get('/graball')
async def grabAll():
    # df = await db.fetch_to_df(f'SELECT history_id FROM history order by ot DESC LIMIT 200')
    df = await db.fetch_to_df(f'SELECT history_id FROM history order by ot DESC LIMIT 50')
    for index, row in df.iterrows():
        id = row['history_id']
        print(id)
        try:
            await grabPgaHis(id)
        except Exception as e:
            print('error : ', e)
    return 'graball'

@app.get('/reload_page')
async def reload_page():
    for connection in connections1:
            await connection.send_json({"type":"reload"})
    return 'Reload All Page Client'

@app.get('/last-eew')
async def last_eew():
    df_eew = await db.fetch_to_df(f''' SELECT * from event_eews ORDER BY "originTime" DESC LIMIT 1; ''')
    json_eew = None if df_eew.empty else json.loads(df_eew.to_json(orient='records', date_format='iso'))[-1]

    return json_eew

@app.get('/history')
async def history():
    df_lspga = pd.DataFrame()
    try:
        df_lspga = await db.fetch_to_df('''
            WITH distinct_history_ids AS (
            SELECT DISTINCT history_id 
            FROM "listPga"
            ORDER BY history_id DESC
            LIMIT 50
        )
        SELECT l.* 
        FROM "listPga" l
        INNER JOIN distinct_history_ids d ON l.history_id = d.history_id
        WHERE l.pga > 0.5
        ORDER BY l.history_id DESC;
        ''')
    except Exception as e:
        print('ada err fecth-> ',e)

    dat = []

    if not df_lspga.empty:
        # print('df ls pga : ', df_lspga)
        u_hist = df_lspga['history_id'].unique()
        # print('u_hist : ', u_hist)
        ls_hist = ', '.join([str(h_id) for h_id in u_hist])
        # print('ls_hist : ', ls_hist)

        qrr = f'''SELECT * FROM history WHERE history_id IN ({ls_hist}); '''
        # print(qrr)

        df_hist = await db.fetch_to_df(qrr)
        df_hist = df_hist.sort_values(by='ot', ascending=False)
        # print(df_hist)
        for index, row in df_hist.iterrows():
            h_id = row['history_id']
            # print('h_id -----> ', h_id)
            pgn_id = row['pgn_id']
            # print('pgn id : ', pgn_id)
            eew_id = row['eew_id']
            ot = row['ot']
            mag = None
            epic = None

            df_pgn = pd.DataFrame()
            df_eew = pd.DataFrame()
            datpga = pd.DataFrame()

            if eew_id:
                try:
                    df_eew = await db.fetch_to_df(f''' SELECT * from event_eews WHERE identifier = '{eew_id}'; ''')
                    mag = float(df_eew.iloc[0]['magnitude'])
                    epic = (float(df_eew.iloc[0]['epicenterLat']), float(df_eew.iloc[0]['epicenterLon']))
                except Exception as e:
                    print('ada err eew-> ',e)

            if pgn_id:
                try:
                    df_pgn = await db.fetch_to_df(f''' SELECT * from event_pgn WHERE "eventPgn_id" = '{pgn_id}'; ''')
                    mag = float(df_pgn.iloc[0]['mag'])
                    epic = (float(df_pgn.iloc[0]['epicLat']), float(df_pgn.iloc[0]['epicLon']))
                except Exception as e:
                    print('ada err pgn-> ',e)

            datpga = df_lspga.loc[df_lspga['history_id'] == h_id].copy()

            # Hitung distance_km dan gunakan .loc[] untuk memastikan aman
            datpga.loc[:, 'distance_km'] = datpga.apply(hitung_jarak1, args=(epic,), axis=1)

            # Filter berdasarkan jarak dan magnitudo
            if mag < 5:
                datpga = datpga[datpga['distance_km'] < 150]
            else:
                datpga = datpga[datpga['distance_km'] < 600]
            
            if datpga.empty:
                continue

            json_pgn = None if df_pgn.empty else json.loads(df_pgn.to_json(orient='records', date_format='iso'))[-1]
            json_eew = None if df_eew.empty else json.loads(df_eew.to_json(orient='records', date_format='iso'))[-1]
            json_pga = None if datpga.empty else json.loads(datpga.to_json(orient='records', date_format='iso'))

            dd = {
                "eew_id": eew_id,
                "history_id": row['history_id'],
                "ot": ot,
                "pgn_id": pgn_id,
                "pgn": json_pgn,
                "eew": json_eew,
                "datapga": json_pga
            }

            dat.append(dd)

        return dat
    return dat

@app.get('/history1/{latMin},{lonMin},{latMax},{lonMax}')
async def history1(latMin: float, lonMin: float, latMax: float, lonMax: float):
    df_lspga = pd.DataFrame()
    try:
        df_lspga = await db.fetch_to_df(f'''
            WITH distinct_history_ids AS (
                SELECT DISTINCT h.history_id 
                FROM history h
                LEFT JOIN "event_pgn" pgn ON h.pgn_id = pgn."eventPgn_id"
                LEFT JOIN "event_eews" eews ON h.eew_id = eews.identifier
                WHERE 
                COALESCE(pgn."epicLat", eews."epicenterLat") BETWEEN {latMin} AND {latMax}
                AND COALESCE(pgn."epicLon", eews."epicenterLon") BETWEEN {lonMin} AND {lonMax}
                ORDER BY h.history_id DESC
                LIMIT 50
            )
            SELECT l.url, l.kode, l.pga, l.pgv, l.pgd, l.lat, l.lon, l.timestamp, l.kab, l."history_id", k.nama 
            FROM "listPga" l
            LEFT JOIN "kota" k ON l.kode = k."Kode"
            INNER JOIN distinct_history_ids d ON l.history_id = d.history_id
            WHERE l.pga > 0.5
            ORDER BY l.history_id DESC;
        ''')
    except Exception as e:
        print('ada err fecth-> ',e)

    dat = []

    if not df_lspga.empty:
        # print('df ls pga : ', df_lspga)
        u_hist = df_lspga['history_id'].unique()
        # print('u_hist : ', u_hist)
        ls_hist = ', '.join([str(h_id) for h_id in u_hist])
        # print('ls_hist : ', ls_hist)

        qrr = f'''SELECT * FROM history WHERE history_id IN ({ls_hist}); '''
        # print(qrr)

        df_hist = await db.fetch_to_df(qrr)
        df_hist = df_hist.sort_values(by='ot', ascending=False)
        # print(df_hist)
        for index, row in df_hist.iterrows():
            h_id = row['history_id']
            # print('h_id -----> ', h_id)
            pgn_id = row['pgn_id']
            # print('pgn id : ', pgn_id)
            eew_id = row['eew_id']
            ot = row['ot']
            mag = None
            epic = None

            df_pgn = pd.DataFrame()
            df_eew = pd.DataFrame()
            datpga = pd.DataFrame()

            if eew_id:
                try:
                    df_eew = await db.fetch_to_df(f''' SELECT * from event_eews WHERE identifier = '{eew_id}'; ''')
                    mag = float(df_eew.iloc[0]['magnitude'])
                    epic = (float(df_eew.iloc[0]['epicenterLat']), float(df_eew.iloc[0]['epicenterLon']))

                except Exception as e:
                    print('ada err eew-> ',e)

            if pgn_id:
                try:
                    df_pgn = await db.fetch_to_df(f''' SELECT * from event_pgn WHERE "eventPgn_id" = '{pgn_id}'; ''')
                    mag = float(df_pgn.iloc[0]['mag'])
                    epic = (float(df_pgn.iloc[0]['epicLat']), float(df_pgn.iloc[0]['epicLon']))
                except Exception as e:
                    print('ada err pgn-> ',e)

            datpga = df_lspga.loc[df_lspga['history_id'] == h_id].copy()

            # Hitung distance_km dan gunakan .loc[] untuk memastikan aman
            datpga.loc[:, 'distance_km'] = datpga.apply(hitung_jarak1, args=(epic,), axis=1)

            if mag < 5:
                datpga = datpga[datpga['distance_km'] < 150]
            else:
                datpga = datpga[datpga['distance_km'] < 600]
            
            if datpga.empty:
                continue

            json_pgn = None if df_pgn.empty else json.loads(df_pgn.to_json(orient='records', date_format='iso'))[-1]
            json_eew = None if df_eew.empty else json.loads(df_eew.to_json(orient='records', date_format='iso'))[-1]
            json_pga = None if datpga.empty else json.loads(datpga.to_json(orient='records', date_format='iso'))

            dd = {
                "eew_id": eew_id,
                "history_id": row['history_id'],
                "ot": ot,
                "pgn_id": pgn_id,
                "pgn": json_pgn,
                "eew": json_eew,
                "datapga": json_pga,
                "feed": None
            }

            if eew_id:        
                dff = await db.fetch_to_df(f'''SELECT * FROM feedback WHERE identifier='{eew_id}' ''') 
                json_feed = json.loads(dff.to_json(orient='records', date_format='iso'))
                dd['feed'] = json_feed


            dat.append(dd)

        return dat
    return dat

@app.get('/eew')
async def eew():
    try:
        df_eew = await db.fetch_to_df('''
            SELECT *
            FROM event_eews
            ORDER BY "originTime" DESC
            LIMIT 20;
        ''')
        
        if df_eew.empty:
            return []
            
        events = json.loads(df_eew.to_json(orient='records', date_format='iso'))
        
        return events
        
    except Exception as e:
        print('Error fetching EEW data:', e)
        return []
    
# ------------- Socket --------------

connections = set()
connections1: Set[WebSocket] = set()

@app.websocket("/ws_pga")
async def ws_pga(websocket: WebSocket):
    await websocket.accept()
    connections.add(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                json_data = json.loads(data)  # Memastikan data adalah JSON yang valid
                # print(json_data)
                for connection in connections:
                    await connection.send_json(json_data)
            except json.JSONDecodeError:
                pass
                # print("Invalid JSON received")
    except Exception as e:
        # print(f"Error: {e}")
        pass
    finally:
        connections.remove(websocket)

@app.websocket("/ws_data")
async def websocket_data(websocket: WebSocket):
    await websocket.accept()
    connections1.add(websocket)
    try:
        while True:
            # Menunggu pesan dari WebSocket, tetapi tidak digunakan di sini
            await websocket.receive_text()
    except WebSocketDisconnect:
        connections1.remove(websocket)


if __name__ == "__main__":
    # uvicorn.run("main2:app", host="0.0.0.0", port=7001, reload=True)
    # uvicorn.run("main2:app", host="0.0.0.0", port=7001, reload=True, log_level="warning")
    uvicorn.run("main2:app", host="0.0.0.0", port=7001, log_level="warning")
    # uvicorn.run("main2:app", host="0.0.0.0", port=7001)