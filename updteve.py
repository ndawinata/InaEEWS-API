from SetDB import Database
import asyncio
INAEWS_DB = {
    'user':'postgres',
    'password':'r00tBMKG2023!',
    'database': 'inaeews',
    'host': 'localhost',
    'port': '5432'
}

db = Database(INAEWS_DB)


async def update_event():
    try:
        # Get the latest event from the database
        event = await db.fetch("SELECT * FROM event")

        for i in event:
            fl = i['file']
            id = i['id']
            eventid = fl.split("_")[0]

            st = await db.fetchone(f"SELECT id FROM event WHERE id = '{id}'")
            if st:
                print('updt : ', await db.execute_return(f''' UPDATE event SET eventid = '{eventid}' WHERE id = '{id}' RETURNING id; '''))
        
        
    except Exception as e:
        print(f"Error updating event: {e}")


if __name__ == "__main__":
    asyncio.run(update_event())