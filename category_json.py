import json
import csv

FILE = "./youtube-new/US_category_id.json"

with open(FILE) as json_input:
    dict1 = json.load(json_input)

output = {}
for item in dict1['items']:
    output[item['id']] = item['snippet']['title']

with open('categories.csv', 'w') as output_file:
    writer = csv.writer(output_file)
    writer.writerow(['category_id', 'category_title'])
    for key in output:
        writer.writerow([key, output[key]])
