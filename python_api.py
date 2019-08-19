from flask_restful import  reqparse, abort, Api, Resource
from flask import Flask
import os

app = Flask(__name__)
api = Api(app)

# argument parsing
parser = reqparse.RequestParser()
parser.add_argument('query')

class ContainerSolutions(Resource):
    def get(self):
        # use parser and find the user's query
        args = parser.parse_args()
        user_query = args['query']

        # create JSON object
        #output = {'prediction': pred_text, 'confidence': confidence}

        output = "Welcome to Python API"

        if None not in user_query:

            if user_query == 'query':
            
                output = "Successfully create table in postgress"
        
        return output

# Setup the Api resource routing here
# Route the URL to the resource
api.add_resource(ContainerSolutions, '/')

if __name__ == '__main__':
    #port = os.environ['FLASK_PORT']
    app.run(host='localhost',debug=True)   