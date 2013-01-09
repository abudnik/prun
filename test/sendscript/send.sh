#!/bin/bash

file=example.py

echo -e `wc -c < $file`"\n"`cat $file` | nc localhost 5555
