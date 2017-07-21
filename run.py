#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join

from hdx.hdx_configuration import Configuration
from hdx.facades.hdx_scraperwiki import facade
from hdx.utilities.downloader import Download

from worldbank import generate_dataset, get_countries, get_indicators_and_tags, get_dataset_date_range

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    downloader = Download()
    indicators, tags = get_indicators_and_tags(base_url, downloader, Configuration.read()['indicator_list'])
    dataset_date_range = get_dataset_date_range(base_url, downloader)

    for countryiso, countryname in get_countries(base_url, downloader):
        dataset = generate_dataset(base_url, countryiso, countryname, indicators, dataset_date_range)
        if dataset is not None:
            logger.info('Adding %s' % countryname)
            dataset.add_tags(tags)
            dataset.update_from_yaml()
            dataset.create_in_hdx()

if __name__ == '__main__':
    facade(main, hdx_site='test', project_config_yaml=join('config', 'project_configuration.yml'))
