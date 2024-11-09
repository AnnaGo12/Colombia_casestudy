#!/bin/sh
pip install numpy
pip install sklearn
pip install pandas

for i in $(seq 1 72); do python main.py; done