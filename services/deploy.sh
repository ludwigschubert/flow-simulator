#!/bin/bash
set -eo pipefail


echo "Copying 'flow' library..."
for dir in */ ; do
  rm -r "${dir}flow" || true;
  cp -r ../flow "${dir}flow";
done

echo "Starting joint deploy..."
yes | gcloud app deploy job_handler/app.yaml --stop-previous-version

echo "Done!"
