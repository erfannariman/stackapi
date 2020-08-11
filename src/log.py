import logging
import os
from datetime import datetime

logging.getLogger().setLevel(logging.INFO)


def set_logging():

    logfilename = "runlog_" + datetime.now().strftime("%Y%m%d") + ".log"
    full_path = os.path.join(os.getcwd(), "log", logfilename)

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        filename=full_path,
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # add the handler to the root logger
    logging.getLogger("").addHandler(console)
