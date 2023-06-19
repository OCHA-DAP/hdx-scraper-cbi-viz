import argparse
import logging
from datetime import datetime
from os import getenv, mkdir
from os.path import join, expanduser
from shutil import rmtree

from cbi import start
from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.retriever import Retrieve

setup_logging()
logger = logging.getLogger()

lookup = "hdx-scraper-cbi-viz"


VERSION = 1.0


def parse_args():
    parser = argparse.ArgumentParser(description="CBi Explorer")
    parser.add_argument("-ua", "--user_agent", default=None, help="user agent")
    parser.add_argument("-pp", "--preprefix", default=None, help="preprefix")
    parser.add_argument("-hs", "--hdx_site", default=None, help="HDX site to use")
    parser.add_argument("-od", "--output_dir", default="output", help="Output folder")
    parser.add_argument(
        "-sd", "--saved_dir", default="saved_data", help="Saved data folder"
    )
    parser.add_argument(
        "-sv", "--save", default=False, action="store_true", help="Save downloaded data"
    )
    parser.add_argument(
        "-usv", "--use_saved", default=False, action="store_true", help="Use saved data"
    )
    parser.add_argument(
        "-wh", "--what", default="ukraine", help="What to run eg. ukraine, turkey"
    )
    args = parser.parse_args()
    return args


def main(
    output_dir,
    saved_dir,
    save,
    use_saved,
    whattorun,
    **ignore,
):
    logger.info(f"##### {lookup} version {VERSION:.1f} ####")
    configuration = Configuration.read()
    output_dir = f"{output_dir}_{whattorun}"
    rmtree(output_dir, ignore_errors=True)
    mkdir(output_dir)
    with Download() as downloader:
        retriever = Retrieve(
            downloader,
            configuration["fallback_dir"],
            f"{saved_dir}_{whattorun}",
            output_dir,
            save,
            use_saved,
        )
        today = datetime.utcnow().isoformat()
        start(
            configuration,
            today,
            retriever,
            output_dir,
            whattorun,
        )


if __name__ == "__main__":
    args = parse_args()
    facade(
        main,
        hdx_read_only=True,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
        output_dir=args.output_dir,
        saved_dir=args.saved_dir,
        save=args.save,
        use_saved=args.use_saved,
        whattorun=args.what,
    )
