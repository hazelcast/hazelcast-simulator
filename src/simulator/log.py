import enum
import re
from datetime import datetime
from threading import Lock

ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
log_lock = Lock()


class Level(enum.Enum):
    info = 1
    warn = 2
    error = 3
    print = 4


def log_host(host, msg, level=Level.info):
    if not msg:
        return

    prefix = f"{host}".ljust(15, " ") + "    "
    log(f"{prefix} {msg}", level=level)


def level_text(level):
    if level == Level.info:
        return "INFO "
    elif level == Level.warn:
        return "WARN "
    elif level == Level.error:
        return "ERROR"
    else:
        return ""


def info(msg):
    log(msg, Level.info)


def error(msg):
    log(msg, Level.error)


def warn(msg):
    log(msg, Level.warn)


def log(msg, level=Level.info):
    if not msg:
        return

    if level == level.print:
        with log_lock:
            for line in msg.splitlines():
                print(line)
        return

    # get rid of color codes
    msg = ansi_escape.sub('', msg)
    dt = datetime.now().strftime("%H:%M:%S")
    level_txt = level_text(level)
    with log_lock:
        for line in msg.splitlines():
            print(f"{level_txt} {dt} {line}")


def log_header(msg):
    remaining = 100 - len(msg)
    if remaining > 0:
        suffix = '-' * remaining
    else:
        suffix = ''
    info(f"-------------[ {msg} ]{suffix}-----")
