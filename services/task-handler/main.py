# Copyright 2015 Google Inc. All Rights Reserved.
# Copyright 2017 Abdul Rauf. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START app]
import logging
import platform
import json

from flask import Flask, request
from flow.event_handler import FileEventHandler

app = Flask(__name__)
event_handler = FileEventHandler()

gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.DEBUG)

@app.route('/handle_file', methods=['POST'])
def handle_file():
  # request.json does not yield data-maybe missing MIME type or sth.
  json_string = request.data
  event = json.loads(json_string)
  src_path = event['src_path']
  event_handler.handle_file_event(src_path)
  app.logger.info("src_path: %s", src_path)
  return '', 200


@app.errorhandler(500)
def server_error(e):
  logging.exception('An error occurred during a request.')
  return """
  An internal error occurred: <pre>{}</pre>
  See logs for full stacktrace.
  """.format(e), 500


if __name__ == '__main__':
  # This is used when running locally. Gunicorn is used to run the
  # application on Google App Engine. See entrypoint in app.yaml.
  app.run(host='127.0.0.1', port=8080, debug=True)
# [END app]
