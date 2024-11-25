#import logging
import os
import time
import json
# from six.moves import range
from kafka import KafkaProducer

import requests




kafka_bootstrap_servers = 'localhost:9092'
kafka_topic_name = 'myFirstTopic'

producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

json_message = None
city_name = None
temperature = None
openweathermap_api_endpoint = None
appid =None

def get_weather_detail(openweathermap_api_endpoint):
    api_response =  requests.get(openweathermap_api_endpoint)
    json_data = api_response.json()
    city_name = json_data["name"]
    humidity = json_data["main"]["humidity"]
    temperature = json_data["main"]["temp"]
    json_message = {"City Name":city_name, "Temperature":temperature, "Humidity":humidity,
                    "Creation Time": time.strftime("%Y-%m-%d %H:%M:%S")}
    return json_message
def get_appid(appid):
    # Return your actual API key here
    return "16ab052ac533d678b7ffbc995f7a8bb5"  # Replace this with your OpenWeatherMap API key
  # Environment variable option
    return os.getenv("16ab052ac533d678b7ffbc995f7a8bb5")


while True:
    city_name = "Chennai"
    appid = get_appid(appid)
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?appid=" + appid + "&q=" + city_name
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print("Published message 1: " + json.dumps(json_message))
    print("Wait for 2 seconds ....")
    time.sleep(2)

    city_name = "Bangalore"
    appid = get_appid(appid)
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?appid=" + appid + "&q=" + city_name
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print("Published message 2: " + json.dumps(json_message))
    print("Wait for 2 seconds ....")
    time.sleep(2)

    city_name = "New Delhi"
    appid = get_appid(appid)
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?appid=" + appid + "&q=" + city_name
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print("Published message 3: " + json.dumps(json_message))
    print("Wait for 2 seconds ....")
    time.sleep(2)

    city_name = "Chennai"
    appid = get_appid(appid)
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?appid=" + appid + "&q=" + city_name
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print("Published message 4: " + json.dumps(json_message))
    print("Wait for 2 seconds ....")
    time.sleep(2)

    city_name = "Chennai"
    appid = get_appid(appid)
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?appid=" + appid + "&q=" + city_name
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print("Published message 5: " + json.dumps(json_message))
    print("Wait for 2 seconds ....")
    time.sleep(2)
