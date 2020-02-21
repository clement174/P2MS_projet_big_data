import requests

content_type = 'application/json'
headers = {'content-type': content_type}

# send http request with image and receive response
api_url = 'http://0.0.0.0:9000/start_pipeline'

print("Send request")
response = requests.post(api_url, data=None, headers=headers)
print(response.status_code)
