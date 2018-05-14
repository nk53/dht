#!/bin/bash

rm *.so
python setup.py build
mv build/lib*/* .
rm -r build/
