#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir, progress_storing_tempdir

from worldbank import get_countries, get_topics, generate_all_datasets_showcases
from worldbank import generate_topline_dataset, generate_resource_view

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-worldbank'


def create_dataset_showcase(dataset, showcase, qc_indicators):
    dataset.update_from_yaml()
    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: World Bank')
    resource_view = generate_resource_view(dataset, qc_indicators)
    if resource_view:
        resource_view.create_in_hdx()
    showcase.create_in_hdx()
    showcase.add_dataset(dataset)


def main():
    """Generate dataset and create it in HDX"""

    with Download(status_forcelist=[400, 429, 500, 502, 503, 504]) as downloader:
        configuration = Configuration.read()
        base_url = configuration['base_url']

        topics = get_topics(base_url, downloader)
        countries = get_countries(base_url, downloader)
        for folder, country in progress_storing_tempdir('WorldBank', countries, 'iso3'):
            dataset, showcase, bites_disabled = generate_all_datasets_showcases(configuration, downloader, folder,
                                                                                country, topics, create_dataset_showcase)
            if dataset is not None:
                dataset.update_from_yaml()
                dataset.generate_resource_view(1, bites_disabled=bites_disabled)
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False)
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)

        with temp_dir('WorldBank') as folder:
            dataset = generate_topline_dataset(base_url, downloader, folder, countries,
                                               configuration['topline_indicators'])
            logger.info('Adding topline indicators')
            dataset.update_from_yaml(path=join('config', 'hdx_topline_dataset_static.yml'))
            dataset.create_in_hdx()


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
