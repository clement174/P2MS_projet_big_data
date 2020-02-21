import requests

content_type = 'application/json'
headers = {'content-type': content_type}

# send http request with image and receive response
api_url = 'http://0.0.0.0:9000/get_logs'
response = requests.get(api_url, data=None, headers=headers)

print(response.content.decode("utf-8"))
