import datetime as dt
import logging
import json

LOG_RECORD_BUILTIN_ATTRS = [
    "msg", "args", "levelno", "processName", "levelname", "name", "pathname", "module", "lineno", "msecs", "relativeCreated", "thread", "threadName", "process", "asctime", "created", "exc_info", "exc_text", "filename", "funcName", "stack_info"
]

class JSONFormatter(logging.Formatter):
    def __init__(self, *, fmt_keys: dict):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    def format(self, record: logging.LogRecord) -> str:
        message = self._prepare_log_dict(record)
        return json.dumps(message)
    
    def _prepare_log_dict(self, record: logging.LogRecord) -> dict:
        always_fields = {
            "message": record.getMessage(),
            "asctime": dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc).isoformat()
        }        
        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)
        
        message = {}
        for key, val in self.fmt_keys.items():
            if val in always_fields:
                message[key] = always_fields.pop(val)
            else:
                message[key] = getattr(record, val)
        message.update(always_fields)

        if record.exc_info is not None:
            always_fields["exception"] = self.formatException(record.exc_info)
        
        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                message[key] = val                

        return message