#!/bin/sh

export PYTHONPATH=src
bin/python -c "from lisk.lisk import run;run()"
