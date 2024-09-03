#! /bin/bash

rm -rf package
rm app.zip
mkdir package
pip install --target package -r requirements.txt
cd $(pwd)/package
zip -r ../app.zip .
cd ..
zip app.zip app.py
