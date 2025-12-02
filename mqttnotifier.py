#! /usr/bin/env python3

import json
import os
import paho.mqtt.client as mqtt
import sys
import signal
import argparse
import toml
import time
import contextlib
import daemon
import jinja2
import notify2
import logging, logging.handlers

APP_NAME="mqttnotifier"


log = logging.getLogger(APP_NAME)
NOTICE = 25
logging.addLevelName(NOTICE, "NOTICE")

MQTT_HOST = "localhost"
MQTT_PORT = 1883
APP_NAME = "mqtt"

BACK_OFF = 0 # starting at 0s
BACK_OFF_PROGRESSION = 1.5
BACK_OFF_CUTOFF = 300 # 5min

DEFAULT_FORMAT = {
    "title": "{{title}}",
    "body": "{{body}}",
    "icon": "",
    "urgency": "normal",
    "muted": False,
    "timeout": 5,
    "category": None,
}

URGENCY_LEVEL = {
    "low": notify2.URGENCY_LOW,
    "normal": notify2.URGENCY_NORMAL,
    "critical": notify2.URGENCY_CRITICAL,
}

NOTIFICATION_DURATION = 15


class Notifier:
    def __init__(self, options):
        self.__dict__.update(options.__dict__)
        self.client = None
        self.back_off = BACK_OFF

    def notify(self, title, body, **hints):

        log.debug(f"Notify with title=[{title}] body=[{body}] hints=[{' '.join(f'{k}={v}' for k,v in hints.items())}]")
        if self.test:
            log.debug("TEST MODE: does not notify")
            return

        notify2.init(APP_NAME)
        n = notify2.Notification(title, body, hints.get("icon", ""))
        for hint,set_hint in [ ("timeout", n.set_timeout),
                               ("urgency", n.set_urgency),
                               ("category", n.set_category) ]:
            if (hint_val := hints.get(hint)) is not None:
                set_hint(hint_val)

        n.show()

        log.debug(f"Notification sent [{title}], title=[{body}]")


    def start(self):
        # Initialize notify connection
        notify2.init(APP_NAME)

        # Connect to mqtt
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        self.client.connect(self.host, self.port, 60)
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            log.info("\nShutting down...")
            self.stop()
        except Exception as e:
            self.stop()
            raise

    def stop(self):
        if self.client:
            self.client.disconnect()
            self.client.loop_stop()
        print("Disconnected")

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            log.info(f"Connected to MQTT broker at {self.host}:{self.port}")
            # Subscribe to all topics
            for topic in self.topics:
                client.subscribe(topic)
                log.info(f"Subscribed to: {topic}")
        else:
            raise f"Connection to MQTT broker failed with code {rc}"

    def on_disconnect(self, client, userdata, rc, properties=None):
        if rc != 0:
            log.error("Unexpected disconnection (code {rc})")

    def find_format(self, topic):
        for t in self.topics:
            log.debug(f"matching [{topic}] as [{t}]")
            if mqtt.topic_matches_sub(t, topic):
                return self.topics[t]
        raise KeyError(topic)

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode('utf-8', errors='ignore')
        log.debug(f"Received {topic}: {payload}")

        # Parse message if it's JSON
        try:
            data = json.loads(payload)
            if isinstance(data, dict):
                title = data.get('title', topic)
                body = data.get('message', str(data))
            else:
                title = topic
                body = payload
        except (json.JSONDecodeError, ValueError):
            title = topic
            body = payload

        try:
            fmt = self.find_format(topic)
        except KeyError:
            log.warning(f"Format for topic [{topic}] not found! Internal bug.")
            fmt = {}

        fmt = DEFAULT_FORMAT | fmt

        log.debug(f"Using format {fmt}")

        if fmt["muted"]:
            log.debug(f"Topic [{topic}] is muted. Do not notify")
            return

        rtitle = jinja2.Template(fmt["title"]).render(title=title, topic=topic, body=body)
        rbody = jinja2.Template(fmt["body"]).render(title=title, topic=topic, body=body)

        hints = {
            "icon": jinja2.Template(fmt["icon"]).render(title=title, topic=topic, body=body),
            "urgency": URGENCY_LEVEL[fmt["urgency"]],
            "timeout": int(fmt["timeout"] * 1000), # convert to ms
            "category": fmt["category"],
        }

        self.notify(rtitle, rbody, **hints)
        log.debug(f"Notification sent for topic [{topic}]")



def main(*args):
    parser = argparse.ArgumentParser()

    parser.add_argument("--config", "-c", help="configuration file")

    parser.add_argument("--topic", "-t", action='append', default=[],
                        dest="topics_list",  nargs="+", metavar=("TOPIC", "TITLE_FORMAT" "BODY_FORMATT"),
                        help="Subscribe to TOPIC with optional FMT (use topic/title/body variables)")

    parser.add_argument("--host", "-H", default=MQTT_HOST)
    parser.add_argument("--port", "-p", default=MQTT_PORT)

    parser.add_argument("--notification-duration", "-n", default=NOTIFICATION_DURATION)

    parser.add_argument("--test", "-T", action="store_true", default=False,
                        help="set log level to debug and do not notify")

    parser.add_argument("--verbose", "-v", action="count", default=3,
                        help="be more verbose (can be used many times)")
    parser.add_argument("--quiet", "-q", action="count", default=0,
                        help="be more quiet (can be used many times)")

    parser.add_argument("--daemon", "-D", action="store_true", default=False)

    options = parser.parse_args(args or None)

    options.topics = toml.load(options.config) if options.config else {}
    options.topics |= { t[0]: dict(zip(["body","title"],t[1:]))
                        for t in options.topics_list }

    options.log_level = max(1, 45+10*(options.quiet-options.verbose)) if not options.test else 1

    if options.daemon:
        handler = logging.handlers.SysLogHandler(address = '/dev/log', facility=logging.handlers.SysLogHandler.LOG_DAEMON)
        formatter = logging.Formatter(fmt=f"{APP_NAME}: %(levelname)-5s: %(message)s")
    else:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(fmt="%(asctime)s: %(levelname)-5s: %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")

    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(options.log_level)

    ctx = daemon.DaemonContext(
        files_preserve=[handler.socket.fileno()]
    ) if options.daemon else contextlib.nullcontext()

    with ctx:
        try:
            if options.daemon or 1:
                log.log(NOTICE, f"Daemon started with PID {os.getpid()}")
            while True:
                try:
                    notifier = Notifier(options)
                    notifier.start()
                except KeyboardInterrupt:
                    pass
                except Exception as e:
                    log.exception("Uncaught error")
                finally:
                    try:
                        notifier.stop()
                    except:
                        pass
                if not options.daemon:
                    break
        finally:
            if options.daemon:
                log.log(NOTICE, f"Daemon stopped (PID {os.getpid()})")


if __name__ == "__main__":
    main()
