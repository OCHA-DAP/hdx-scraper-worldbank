#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.facades.simple import facade
from hdx.utilities.downloader import Download, DownloadError
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from tenacity import (
    after_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)
from worldbank import (
    generate_all_datasets_showcases,
    generate_topline_dataset,
    get_countries,
    get_topics,
)

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-worldbank"


def create_dataset_showcase(dataset, showcase, qc_indicators, batch):
    dataset.update_from_yaml()
    dataset.generate_resource_view(-1, indicators=qc_indicators)
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

    with Download(status_forcelist=[400, 429, 500, 502, 503, 504]) as downloader:
        with wheretostart_tempdir_batch(lookup) as info:
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
                path=join("config", "hdx_topline_dataset_static.yml")
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
                before=after_log(logger, logging.INFO),
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
                    dataset.update_from_yaml()
                    dataset.generate_resource_view(
                        -1,
                        bites_disabled=bites_disabled,
                        indicators=combined_qc_indicators,
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script="HDX Scraper: WorldBank",
                        batch=batch,
                    )
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)

            for _, nextdict in progress_storing_folder(info, countries, "iso3"):
                process_country(nextdict)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
