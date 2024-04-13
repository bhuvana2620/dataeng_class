import requests

def get_location(api_key, city_name, state_name):
    geo_url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},{state_name}&limit=2&appid={api_key}"
    geo_response = requests.get(geo_url)
    geo_data = geo_response.json()
    if geo_data:
        location = geo_data[0]  
        lat = location["lat"]
        lon = location["lon"]
        return lat, lon
    else:
        print("No data found")
        return None, None

def get_current_weather(api_key, lat, lon):
    weather_endpoint = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
    weather_response = requests.get(weather_endpoint)
    weather_data = weather_response.json()
    if weather_data["cod"] != "404":
        main_info = weather_data["main"]
        weather_info = weather_data["weather"]
        weather_description = weather_info[0]["description"]
        return weather_description
    else:
        print("City Not Found")
        return None

def get_forecast(api_key, lat, lon, date):
    forecast_endpoint = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}"
    forecast_response = requests.get(forecast_endpoint)
    if forecast_response.status_code == 200:
        forecast_data = forecast_response.json()
        if forecast_data["cod"] == "200":
            for forecast in forecast_data["list"]:
                if forecast["dt_txt"] == date:
                    weather_desc = forecast["weather"][0]["description"]
                    return weather_desc
            print("No forecast data found for the specified date.")
            return None
        else:
            print("Error occurred during retrieving forecast data.")
            return None
    else:
        print("Error occurred during retrieving forecast data.")
        return None

def main():
    api_key = "8282be316103ad9447f74e44b0f16c7c"
    city_name = "portland"
    state_name = "Oregon"
    lat, lon = get_location(api_key, city_name, state_name)
    if lat is not None and lon is not None:
        current_weather = get_current_weather(api_key, lat, lon)
        if current_weather is not None:
            if current_weather.lower() == 'rain':
                print("It is raining in Portland, OR.")
            else:
                print("It is not raining in Portland, OR.")

        date_to_check = "2024-04-15 15:00:00"
        forecast = get_forecast(api_key, lat, lon, date_to_check)
        if forecast is not None:
            if forecast.lower() == 'rain':
                print("For our next class it will be raining.")
            else:
                print("For our next class it won't be raining. Weather is:", forecast)

if __name__ == "__main__":
    main()
