#!/bin/bash
pip install -r requirements.txt
python manage.py tailwind build
python manage.py collectstatic --noinput
