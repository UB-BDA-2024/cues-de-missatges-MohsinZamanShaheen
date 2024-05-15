from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
import json
from shared.mongodb_client import MongoDBClient
from shared import redis_client
from shared.sensors import models, schemas
from shared import timescale
from shared.elasticsearch_client import ElasticsearchClient
from shared.cassandra_client import CassandraClient 
from datetime import datetime, timedelta

import logging 



class DataCommand():
    def __init__(self, from_time, to_time, bucket):
        if not from_time or not to_time:
            raise ValueError("from_time and to_time must be provided")
        if not bucket:
            bucket = 'day'
        self.from_time = from_time
        self.to_time = to_time
        self.bucket = bucket
        
        
# MY REPOSITORY COMBINED

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_specific(db: Session, sensor_id: int, mongodb_client: MongoDBClient) -> Optional[models.Sensor]:
    db_sensor =  db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        return None

    # Fetch the sensor data from MongoDB
    mongodb_client.getDatabase("MongoDB_")
    mongodb_collection = mongodb_client.getCollection("sensors")
    document_sensor_data = mongodb_collection.find_one({"id_sensor": sensor_id})

    if document_sensor_data is None:
        return None
    
    document_sensor_data['latitude'] = document_sensor_data['location']['coordinates'][1]
    document_sensor_data['longitude'] = document_sensor_data['location']['coordinates'][0]
    document_sensor_data = {k: v for k, v in document_sensor_data.items() if k not in ['id_sensor', '_id', 'location']}

    # Merge the data from the SQL database and MongoDB
    sensor_data = {**db_sensor.to_dict(), **document_sensor_data}
    print("Sensor Data in get sensor", sensor_data )

    return sensor_data

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: MongoDBClient,  es: ElasticsearchClient, cassandra:CassandraClient) -> models.Sensor:
    # SQL -> save only identifier and name
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    
    #Mongo -> save the document with static attributes.
    document_sensor_data = {
        "id_sensor": db_sensor.id,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        # referenced from MongoDB geospatial queries documentation
        "location": {
            "type": "Point",
            "coordinates": [sensor.longitude, sensor.latitude]
        },
        "description": sensor.description
    }
    
    mongodb_client.getDatabase("MongoDB_")
    mongodb_collection = mongodb_client.getCollection("sensors")
    mongodb_collection.insert_one(document_sensor_data)
    
    #document_to_index = {k: v for k, v in document_sensor_data.items() if k not in ['_id']}   
    document_to_index = {
        "id_sensor": db_sensor.id,
        "name": sensor.name,
        "type": sensor.type,
        "description": sensor.description
    } 
    
      # Index the document in Elasticsearch
    es.index_document("sensors", document_to_index)

    sensor_data = sensor.dict()
    sensor_data.update({"id":db_sensor.id}) 
    
    cassandra.execute(f"""
        UPDATE sensor.sensor_count_by_type
        SET count = count + 1 
        WHERE sensor_type = '{sensor.type}';
        """
    )
    
    return sensor_data


def record_data(cassandra: CassandraClient,redis: redis_client, timescale: timescale, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    # Ensure proper handling of None values which can't be inserted directly into CQL query strings
    temperature = 'NULL' if data.temperature is None else data.temperature
    humidity = 'NULL' if data.humidity is None else data.humidity
    velocity = 'NULL' if data.velocity is None else data.velocity
    battery_level = 'NULL' if data.battery_level is None else data.battery_level
    timestamp = f"'{data.last_seen}'" if data.last_seen is not None else 'NULL'
    
    ## save in redis
    sensorData = json.dumps(data.dict())
    redis.set(f"sensor:{sensor_id}:data", sensorData)
    
    # save in timescale
    query = f"""
        INSERT INTO sensor_data (sensor_id, velocity, temperature, humidity, battery_level, time)
        VALUES ({sensor_id}, {velocity}, {temperature}, {humidity}, {data.battery_level}, '{data.last_seen}')
        """
    timescale.execute(query)
    timescale.conn.commit()
    
    # save in cassandra
    
    logging.debug(f"Recording data for Sensor ID {sensor_id} with data: {data}")

    # Construct the insert query using safer formatting to prevent SQL injection-like issues
    insert_query = f"""
    INSERT INTO sensor_data_tbl (sensor_id, timestamp, temperature, humidity, velocity, battery_level)
    VALUES ({sensor_id}, {timestamp}, {temperature}, {humidity}, {velocity}, {battery_level});
    """
    try:
        cassandra.execute(insert_query)
        print("Data inserted successfully into Cassandra.")
        if temperature is not None:
            update_temperature_statistics(cassandra, sensor_id, temperature)
        if battery_level is not None:
            insert_query = f"""
            INSERT INTO low_battery_sensors (sensor_id, battery_level, last_update)
            VALUES ({sensor_id}, {battery_level}, {timestamp});
            """
            cassandra.execute(insert_query)
            print("Low battery sensor data inserted successfully into Cassandra.")
        
    except Exception as e:
        print(f"Failed to insert data into Cassandra: {e}")

# GET DATA indexos version

def get_data(redis: redis_client.RedisClient, sensor_id: int, db:Session) -> schemas.Sensor:
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    sensorDataDB = redis.get( f"sensor:{sensor_id}:data")
    sensor_data = json.loads(sensorDataDB.decode())
    print("GETTING THIS?", db_sensor.id)
    print("sensor data?", sensor_data)
    sensor_data['id'] = db_sensor.id
    sensor_data['name'] = db_sensor.name
    return sensor_data

# GET DATA temporal version

def get_data_timescale(timescale: timescale, sensor_id: int, from_date: str, to_date: str, bucket: str) -> schemas.Sensor:
    # Determine the appropriate materialized view based on the bucket
    if bucket is None or bucket == 'hour':
        materialized_view = 'sensor_data_hourly'
    elif bucket == 'day':
        materialized_view = 'sensor_data_daily'
    elif bucket == 'week':
        materialized_view = 'sensor_data_weekly'
    elif bucket == 'month':
        materialized_view = 'sensor_data_monthly'
    elif bucket == 'year':
        materialized_view = 'sensor_data_yearly'
    else:
        raise HTTPException(status_code=400, detail="Invalid bucket value")
    
    # Refresh the materialized view
    timescale.refresh_materialized_view(materialized_view)

    # Parse the input date strings
    from_date_dt = datetime.fromisoformat(str(from_date))
    to_date_dt = datetime.fromisoformat(str(to_date))

    # Construct the query based on the bucket interval
    if bucket == 'week':
        # Calculate start and end of the week
        start_of_week = from_date_dt - timedelta(days=from_date_dt.weekday())
        end_of_week = to_date_dt + timedelta(days=6 - to_date_dt.weekday())
        query_condition = f"week >= '{start_of_week.isoformat()}' AND week <= '{end_of_week.isoformat()}'"
    else:
        # For other buckets, use standard between clause
        query_condition = f"{bucket} between '{from_date}' and '{to_date}'"

    # Construct the query
    query = f"""
    SELECT
        {bucket},
        avg_velocity,
        avg_temperature,
        avg_humidity,
        avg_battery_level
    FROM
        {materialized_view}
    WHERE
        sensor_id = {sensor_id}
        AND {query_condition}
    """
    
    # Execute the query
    timescale.execute(query)
    result = timescale.cursor.fetchall()

    # Convert the result into a dictionary format
    sensor_data = {}
    for row in result:
        bucket_time, avg_velocity, avg_temperature, avg_humidity, avg_battery_level = row
        sensor_data[bucket_time] = {
            "avg_velocity": avg_velocity,
            "avg_temperature": avg_temperature,
            "avg_humidity": avg_humidity,
            "avg_battery_level": avg_battery_level
        }
    return sensor_data


def update_temperature_statistics(cassandra, sensor_id, temperature):
    print("Fetching existing data for sensor ID:", sensor_id)
    query = """
    SELECT max_temperature, min_temperature, total_temperature, temperature_count
    FROM temperature_statistics
    WHERE sensor_id = %s;
    """
    result = cassandra.execute(query, [sensor_id])
    row = result.one()

    if row:
        max_temp = max(row.max_temperature, temperature)
        min_temp = min(row.min_temperature, temperature)
        new_count = row.temperature_count + 1
        new_total = row.total_temperature + temperature
        avg_temp = new_total / new_count
        print(f"Updating stats: Max Temp: {max_temp}, Min Temp: {min_temp}, Avg Temp: {avg_temp}")
    else:
        max_temp = min_temp = avg_temp = temperature
        new_total = temperature
        new_count = 1
        print("No existing record found. Setting initial values.")

    update_query = """
    INSERT INTO temperature_statistics (sensor_id, max_temperature, min_temperature, avg_temperature, total_temperature, temperature_count)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cassandra.execute(update_query, (sensor_id, max_temp, min_temp, avg_temp, new_total, new_count))
    print("Temperature statistics updated successfully.")

        
def get_temperature_values(cassandra:CassandraClient, db: Session, mongodb_client: MongoDBClient):
 
    query = """
    SELECT sensor_id, max_temperature, min_temperature, avg_temperature
    FROM temperature_statistics;
    """
    try:
        result = cassandra.execute(query)
        print("Result", result)
        sensors = []
        for row in result:
            print("Row",row.sensor_id)
            db_sensor = get_sensor_specific(db, row.sensor_id, mongodb_client)  
            sensors.append({
                "id": row.sensor_id,
                "name": db_sensor['name'],
                "latitude": db_sensor['latitude'],
                "longitude": db_sensor['longitude'],
                "type": db_sensor['type'],
                "mac_address": db_sensor['mac_address'],
                "manufacturer": db_sensor['manufacturer'],
                "model": db_sensor['model'],
                "serie_number": db_sensor['serie_number'],
                "firmware_version": db_sensor['firmware_version'],
                "description": db_sensor['description'],
                "values": [{
                    "max_temperature": row.max_temperature,
                    "min_temperature": row.min_temperature,
                    "average_temperature": row.avg_temperature
                }]
            })
        return {"sensors": sensors}
    except Exception as e:
        print(f"Failed to fetch temperature values: {e}")
        return {"sensors": []}



def get_sensors_quantity(db: Session, cassandra:CassandraClient):
    print("Getting sensors quantity")
    query = """
    SELECT *
    FROM sensor_count_by_type;
    """
    result = cassandra.execute(query)
    print("Result", result)
    sensors = []
    for row in result:
        print("Row", row)
        sensors.append({
            "type": row.sensor_type,
            "quantity": row.count
        })
        
    return {"sensors": sensors}



def get_low_battery_sensors(cassandra:CassandraClient, db: Session, mongodb_client: MongoDBClient):
    query = """
    SELECT sensor_id, battery_level, last_update
    FROM low_battery_sensors
    WHERE battery_level <= 0.2
    ALLOW FILTERING;
    """
    try:
        result = cassandra.execute(query)
        sensors = []
        for row in result:
            db_sensor = get_sensor_specific(db, row.sensor_id, mongodb_client)
            sensors.append({
                "id": row.sensor_id,
                "name": db_sensor['name'],
                "latitude": db_sensor['latitude'],
                "longitude": db_sensor['longitude'],
                "type": db_sensor['type'],
                "mac_address": db_sensor['mac_address'],
                "manufacturer": db_sensor['manufacturer'],
                "model": db_sensor['model'],
                "serie_number": db_sensor['serie_number'],
                "firmware_version": db_sensor['firmware_version'],
                "description": db_sensor['description'],
                "battery_level": round(row.battery_level, 2)
            })
        print({"sensors": sensors})
        return {"sensors": sensors}
    except Exception as e:
        print(f"Failed to fetch low battery sensors: {e}")
        return {"sensors": []}


def get_sensors_near(mongodb_client: MongoDBClient,  db:Session, redis:redis_client.RedisClient,  latitude: float, longitude: float, radius: int):
    mongodb_client.getDatabase("MongoDB_")
    collection = mongodb_client.getCollection("sensors")
    #enable geospatial queries
    collection.create_index([("location", "2dsphere")])
    nearby_sensors = list(collection.find(
        {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                },
                "$maxDistance": radius
            }
        }
    }
    ))
    #print("Nearby are: ", nearby_sensors)
    sensors = []
    for doc in nearby_sensors:
        doc["_id"] = str(doc["_id"])
        sensor = get_sensor_specific(db=db, sensor_id=doc["id_sensor"], mongodb_client=mongodb_client).__dict__
        sensor_redis = get_data(redis=redis, sensor_id=doc["id_sensor"], db=db)
        if sensor is not None:
            sensor = {**sensor, **sensor_redis} 
            sensors.append(sensor)
    return sensors if sensors else []




def delete_sensor(db: Session, sensor_id: int, redis: redis_client, mongodb_client: MongoDBClient):
    """
    Delete a sensor from postgreSQL, Redis, and MongoDB.
    """
    # delete from redis
    redis.delete(f"sensor:{sensor_id}:data")
    # delete from 
    mongodb_client.getDatabase('MongoDB_')
    mongodb_client.getCollection('sensors')
    mongodb_client.collection.delete_one({"id_sensor": sensor_id})
    # delete from posgreSQL
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor



def search_sensors(db: Session, mongodb_client: MongoDBClient, es: ElasticsearchClient, query: str, size: int, search_type: str):
    try:
        query_dict = json.loads(query)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid  JSON format in query parameter")

    if search_type not in ["match", "prefix", "similar"]:
        raise HTTPException(status_code=400, detail="Invalid search_type. Allowed values: 'match', 'prefix', 'similar'")
    
    es_query = {
        "size": size
    }
    field_name = list(query_dict.keys())[0]
    field_value = list(query_dict.values())[0]
    
    if search_type == "match":
        es_query["query"] = {
            "multi_match": {
                "query": field_value,
                "fields": ["name", "description", "type"]
            }
        }
    elif search_type == "prefix":
        es_query["query"] = {
            "prefix": {
                field_name+ ".keyword": f"{field_value}"
            }
        }
    elif search_type == 'similar':
        es_query["query"]  = {
            "match": {
                field_name: {
                    "query": field_value,
                    "fuzziness": 'auto',
                    'operator': 'and'
                }
            }
        }

    print("Elasticsearch query: ", es_query)
    results = es.search("sensors", es_query)
    sensors = [hit["_source"] for hit in results["hits"]["hits"]]
    
    print("Sensors: ", sensors)
    
    formatted_sensors = []
    for sensor in sensors:
        new_sensor = get_sensor_specific(db, sensor["id_sensor"], mongodb_client)
        formatted_sensors.append(new_sensor)
    
    return formatted_sensors

        
        

def add_sensor_to_postgres(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    date = datetime.now()

    db_sensor = models.Sensor(name=sensor.name, joined_at=date)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    return db_sensor


def add_sensor_to_mongodb(mongodb_client: MongoDBClient, db_sensor: schemas.SensorCreate, id):
    mongo_projection = schemas.SensorMongoProjection(id=id, name=db_sensor.name, location={'type': 'Point',
                                                                                           'coordinates': [
                                                                                               db_sensor.longitude,
                                                                                               db_sensor.latitude]},
                                                     type=db_sensor.type, mac_address=db_sensor.mac_address,
                                                     description=db_sensor.description,
                                                     serie_number=db_sensor.serie_number,
                                                     firmware_version=db_sensor.firmware_version, model=db_sensor.model,
                                                     manufacturer=db_sensor.manufacturer)
    mongodb_client.getDatabase()
    mongoInsert = mongo_projection.dict()
    mongodb_client.getCollection().insert_one(mongoInsert)
    return mongo_projection.dict()

def getView(bucket: str) -> str:
    if bucket == 'year':
        return 'sensor_data_yearly'
    if bucket == 'month':
        return 'sensor_data_monthly'
    if bucket == 'week':
        return 'sensor_data_weekly'
    if bucket == 'day':
        return 'sensor_data_daily'
    elif bucket == 'hour':
        return 'sensor_data_hourly'
    else:
        raise ValueError("Invalid bucket size")
