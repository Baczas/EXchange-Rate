from flask import *
from google.cloud import bigquery
from exchange import API



app = Flask(__name__)

@app.get("/rate/<curr>/<date>/<value>")
def rate(curr, value, date):
    table_name = '<project_id>.<dataset>.<table_name>'
    credentials = 'credentials.json'
    v = API.validate(curr, value, date)
    if v is not None:
        yield v
    rating = API.count_rate(curr, value, date, table_name, credentials)

    yield "<br>".join(rating)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
