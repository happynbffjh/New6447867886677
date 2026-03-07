#!/bin/bash
# Start the bot in the background
python bot.py &

# Start the web server in the foreground
# (The foreground process keeps the container alive)
gunicorn app:app
