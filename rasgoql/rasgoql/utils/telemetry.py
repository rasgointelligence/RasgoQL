import json
import contextlib
from multiprocessing.pool import ThreadPool
import atexit
from typing import Optional

import requests

HEAP_KEY = "2288014214"
HEAP_URL = "https://heapanalytics.com/api"

thread_pool = ThreadPool(2)
atexit.register(thread_pool.terminate)


def failure_telemetry(obj, name):
    attr = object.__getattribute__(obj, name)
    if hasattr(attr, "__call__") and not name.startswith("_") and not name.startswith("connect"):
        def logged_function(*args, **kwargs):
            tracked_properties = {
                "source": "RasgoQL",
                "class": obj.__class__.__name__,
                "module": attr.__module__,
                "method": attr.__name__,
            }

            try:
                result = attr(*args, **kwargs)
            except Exception as err:
                tracked_properties["execution_status"] = "failed"
                thread_pool.apply_async(track_call, args=(tracked_properties, str(err)))
                raise err from None
            else:
                tracked_properties["execution_status"] = "completed"
                thread_pool.apply_async(track_call, args=(tracked_properties, None))
                return result

        return logged_function
    return attr


def track_call(tracked_properties: dict, error_message: Optional[str]):
    """
    Send a "track" event to the Heap Analytics API server.
    :param tracked_properties:  contains event data for heap to track
    :param error_message: if the called function fails, this string will
    be the failure message
    """
    data = {
        "app_id": HEAP_KEY,
        # TODO: implement cookies and opt-out for individual level tracking
        "identity": "user",
        "event": tracked_properties["method"],
        "properties": tracked_properties,
    }
    if error_message:
        data["properties"]["error_message"] = error_message

    with contextlib.suppress(Exception):
        requests.post(
            url=f"{HEAP_URL}/track",
            data=json.dumps(data),
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
