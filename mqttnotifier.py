#! /usr/bin/env python3

import json
import os
import paho.mqtt.client as mqtt
from dbus import SessionBus, Interface
from dbus.exceptions import DBusException
import sys
import signal
import argparse
import toml
import time
import contextlib
import daemon
import jinja2

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
}

NOTIFICATION_DURATION = 15


class Notifier:
    def __init__(self, options):
        self.__dict__.update(options.__dict__)
        self.client = None
        self.back_off = BACK_OFF
        self.connect_dbus()

    def connect_dbus(self):
        # Connect to dbus
        self.bus = SessionBus()
        notify_obj = self.bus.get_object(
            'org.freedesktop.Notifications',
            '/org/freedesktop/Notifications'
        )
        self.notify_interface = Interface(
            notify_obj,
            'org.freedesktop.Notifications'
        )
        log.info("Connected to D-Bus notification interface.")

    def notify(self, title, body):

        log.debug(f"Notify with title=[{title}] body=[{body}]")
        if self.test:
            log.debug("TEST MODE: does not notify")
            return

        try:
            # Send notification
            # Parameters: app_name, replaces_id, app_icon, title, body, actions, hints, timeout
            while True:
                try:
                    self.notify_interface.Notify(
                        APP_NAME,
                        0,  # replaces_id (0 = don't replace)
                        "",  # app_icon
                        title,
                        body,
                        [],  # actions
                        {},  # hints
                        self.notification_duration*1000 # in ms
                    )
#                    self.notify_interface.Notify(APP_NAME, title, body, app_icon = "", actions = [], hints = {},
#                                                 timeout = self.notification_duration*1000)  # in ms
                except DBusException as e:
                     if "ServiceUnknown" in str(e):
                         log.warning("Notification service disconnected, attempting to reconnect...")
                         time.sleep(self.back_off)
                         self.back_off = 1 if not self.back_off else min(self.back_off*BACK_OFF_PROGRESSION, BACK_OFF_CUTOFF)
                         self.connect_dbus()
                     else:
                         raise
                else:
                    break

            self.back_off = BACK_OFF
            log.debug(f"Notification sent [{title}], title=[{body}]")

        except DBusException as e:
            log.error(f"Failed to send notification: {e}")
        except Exception as e:
            raise

    def start(self):
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
        rtitle = jinja2.Template(fmt["title"]).render(title=title, topic=topic, body=body)
        rbody = jinja2.Template(fmt["body"]).render(title=title, topic=topic, body=body)

        self.notify(rtitle, rbody)
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
                    log.exception(e, "Uncaught error")
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
