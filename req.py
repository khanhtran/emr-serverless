import requests

END_POINT ='https://q652q8ycce.execute-api.us-east-2.amazonaws.com/dev'

get_response = requests.get(END_POINT)

print(get_response)
print(get_response.content)

post_response = requests.post(END_POINT)

print(post_response)
print(post_response.content)