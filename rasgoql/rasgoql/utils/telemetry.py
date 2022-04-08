import json
import logging
import requests

HEAP_KEY = "2288014214"
HEAP_URL = "https://heapanalytics.com/api"

def failure_telemetry(obj, name):
    attr = object.__getattribute__(obj, name)
    if hasattr(attr, '__call__') and not name.startswith("_") and not name.startswith("connect"):
        def tracked_function(*args, **kwargs):
            print(f"class:\n{obj.__class__.__name__}")
            print(f"module:\n{attr.__module__}")
            print(f"method:\n{attr.__name__}")
            try:
                track_call(
                    app_id=HEAP_KEY,
                    identity="user",
                    event=attr.__name__,
                    properties={
                        "source": "RasgoQL",
                        "class": obj.__class__.__name__,
                        "module": attr.__module__,
                        "method": attr.__name__,
                        "execution_status": "TODO: determine failure",
                        "userId": "TODO: user ID?",
                    }
                )
            except Exception as e:
                logging.info(f"Called {attr.__name__} with parameters: {kwargs}")
                logging.info(e)
            return attr(*args, **kwargs)
        return tracked_function
    # TODO: add try except here, and catch exceptions and throw to a failure logger?
    return attr


def track_call(app_id: str,
               identity: int,
               event: str,
               properties: dict = None):
    """
    Send a "track" event to the Heap Analytics API server.
    :param identity: unique id used to identify the user
    :param event: event name
    :param properties: optional, additional event properties
    """
    data = {
        "app_id": app_id,
        "identity": identity,
        "event": event
    }

    if properties is not None:
        data["properties"] = properties

    response = requests.post(url=f"{HEAP_URL}/track",
                             data=json.dumps(data),
                             headers={"Content-Type": "application/json"})
    response.raise_for_status()
    return response
