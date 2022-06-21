#!/usr/bin/sh

PIPDIR=.venv/lib/python3.10/site-packages
cd $PIPDIR
zip -r ../../../../lambda-pkg.zip .
cd ../../../../src
zip -g ../lambda-pkg.zip lambda_function.py telegram.py 