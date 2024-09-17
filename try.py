import json

with open('config.json', 'r') as file:
    key = json.load(file)

print(key['project']['title'])