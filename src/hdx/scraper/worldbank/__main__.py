#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.data.user import User
from hdx.facades.simple import facade
from hdx.utilities.downloader import Download, DownloadError
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from tenacity import (
    after_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

from hdx.scraper.worldbank._version import __version__
from hdx.scraper.worldbank.pipeline import (
    generate_all_datasets_showcases,
    generate_topline_dataset,
    get_countries,
    get_topics,
)

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-worldbank"
_UPDATED_BY_SCRIPT = "HDX Scraper: WorldBank"


def create_dataset_showcase(dataset, showcase, qc_indicators, batch):
    dataset.update_from_yaml(
        script_dir_plus_file(join("config", "hdx_dataset_static.yaml"), main)
    )
    dataset.generate_quickcharts(-1, indicators=qc_indicators)
    dataset.create_in_hdx(
        remove_additional_resources=True,
        hxl_update=False,
        updated_by_script="HDX Scraper: World Bank",
        batch=batch,
    )
    showcase.create_in_hdx()
    showcase.add_dataset(dataset)


def main():
    """Generate dataset and create it in HDX"""

    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access("707b1f6d-5595-453f-8da7-01770b76e178")

    with Download(status_forcelist=[400, 429, 500, 502, 503, 504]) as downloader:
        with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
            folder = info["folder"]
            batch = info["batch"]
            configuration = Configuration.read()
            base_url = configuration["base_url"]
            combined_qc_indicators = configuration["combined_qc_indicators"]
            topics = get_topics(base_url, downloader)
            countries = get_countries(base_url, downloader)
            logger.info(f"Number of countries: {len(countries)}")

            dataset = generate_topline_dataset(
                base_url,
                downloader,
                folder,
                countries,
                configuration["topline_indicators"],
            )
            logger.info("Adding topline indicators")
            dataset.update_from_yaml(
                script_dir_plus_file(
                    join("config", "hdx_topline_dataset_static.yaml"), main
                )
            )
            dataset.create_in_hdx(
                remove_additional_resources=True,
                hxl_update=False,
                updated_by_script="HDX Scraper: WorldBank",
                batch=batch,
            )

            @retry(
                retry=(
                    retry_if_exception_type(DownloadError)
                    | retry_if_exception_type(HDXError)
                ),
                stop=stop_after_attempt(5),
                wait=wait_fixed(3600),
                after=after_log(logger, logging.INFO),
            )
            def process_country(nextdict):
                dataset, showcase, bites_disabled = generate_all_datasets_showcases(
                    configuration,
                    downloader,
                    folder,
                    nextdict,
                    topics,
                    create_dataset_showcase,
                    batch,
                )
                if dataset is not None:
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    dataset.generate_quickcharts(
                        -1,
                        bites_disabled=bites_disabled,
                        indicators=combined_qc_indicators,
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script=_UPDATED_BY_SCRIPT,
                        batch=batch,
                    )
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)

            for _, nextdict in progress_storing_folder(info, countries, "iso3"):
                process_country(nextdict)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
