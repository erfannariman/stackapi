import yaml
import sentry_sdk

sentry_sdk.init(
    "https://53a8bd82d4b04e44affb127b2c5d06ce@o408709.ingest.sentry.io/5407797",
    traces_sample_rate=1.0,
)


def get_settings(file_name):
    with open(file_name, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
