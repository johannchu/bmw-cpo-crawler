#-*- coding: utf-8
import requests
from flask import Flask, request, Response, jsonify
import cloud_sql_connector

# initialize the flask app
app = Flask(__name__)

db = cloud_sql_connector.init_connection_engine()
bmw_table = cloud_sql_connector.get_bmw_table()

def crawl_bmw_cpo(req):
    url = 'https://bps.bmw.com.tw/api/search'
    
    x = requests.post(url, data = req)
    resp_dict = x.json()
    return resp_dict

def prepare_mail_content(newly_added_cars):
    content = "Hi,\n\nThis is your notification of newly added cars in BMW inventory:\n\n"
    for id_ in newly_added_cars:
        car = newly_added_cars[id_]
        content += "URL: " + "https://bps.bmw.com.tw/car/show/" + str(car['id']) + "\n"
        content += "Generation: " + car['source_no'] + "\n"
        content += "Year: " + str(car['fac_year']) + "\n"
        content += "Mileage: " + str(car['mileage']) + "\n"
        content += "Model: " + car['model_name'] + "\n"
        content += "Price: " + str(car['price']) + "萬\n"
        content += "Color: " + car['color_category_name'] + "\n"
        content += "Store name: " + car['show_name'] + "\n" 
        content += "Show room: " + car['store'] + "\n\n"
    return content

def send_email_notification(newly_added_cars):
    import mail_service
    service = mail_service.init_gmail_service_with_gcs()
    mail_content = prepare_mail_content(newly_added_cars)
    message_payload = mail_service.create_message('sender@foo.com', 'receiver@bar.com', \
                                 'Notification from BMW CPO', mail_content)
    mail_service.send_message(service, 'me', message_payload)
    return None

# default route
@app.route('/', methods=['GET'])
def index():
    return 'Hello World!'

# create a route for webhook
@app.route('/webhook', methods=['POST'])
def webhook(request):
    # return response
    req = request.get_json(force=True)
    resp_dict = crawl_bmw_cpo(req)
    newly_added_cars = cloud_sql_connector.process_crawl_result(db, bmw_table, resp_dict)
    if len(newly_added_cars) > 0:
        print ('Found new cars in the inventory. Sending notification now......')
        send_email_notification(newly_added_cars)
    return Response(status=200)

# run the app
if __name__ == '__main__':
   app.run(host="0.0.0.0")