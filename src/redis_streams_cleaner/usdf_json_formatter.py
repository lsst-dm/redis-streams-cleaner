import json
import logging

__all__ = ("UsdfJsonFormatter",)


class UsdfJsonFormatter(logging.Formatter):
    """A formatter that can be parsed by the Loki/Grafana system at USDF.

    The formatter's output is a JSON-encoded message with "flattened" metadata
    to make it easy to inspect with Grafana filters.

    Parameters
    ----------
    labels : `dict` [`str`, `str`]
        Any metadata that should be attached to the log.
    """

    def format(self, record: logging.LogRecord) -> str:
        msg = super().format(record)
        entry = {
            "asctime": self.formatTime(
                record, datefmt="%Y-%m-%dT%H:%M:%S.%(msecs)03d%z"
            )
            % {"msecs": record.msecs},
            "funcName": record.funcName,
            "level": record.levelname,
            "levelno": record.levelno,
            "lineno": record.lineno,
            "message": msg,
            "name": record.name,
            "pathname": record.pathname,
            "process": record.process,
            "thread": record.thread,
        }
        return json.dumps(entry)
