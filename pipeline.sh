#!/bin/bash

echo "Starting to run Pipeline ..."

. ~/iga_utils/bin/activate

python /home/flask/iga_utils/myapp/modules/datastudiopipeline.py

echo "... Pipeline concluído."