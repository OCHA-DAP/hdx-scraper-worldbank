#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join
from tempfile import gettempdir

from hdx.hdx_configuration import Configuration
from hdx.facades.hdx_scraperwiki import facade
from hdx.utilities.downloader import Download

from worldbank import generate_dataset, get_countries, get_indicators_and_tags, generate_topline_dataset

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    with Download(status_forcelist=[400, 429, 500, 502, 503, 504]) as downloader:
        indicators, tags = get_indicators_and_tags(base_url, downloader, Configuration.read()['indicator_list'])
        folder = gettempdir()
        topline_indicator_names = Configuration.read()['topline_indicators']

        country_isos = list()
        topline_indicators = list()
        for countryiso, countryname in get_countries(base_url, downloader):
            dataset, country_topline_indicators = generate_dataset(base_url, downloader, countryiso, countryname,
                                                                   indicators, topline_indicator_names)
            if dataset is not None:
                logger.info('Adding %s' % countryname)
                dataset.add_tags(tags)
                dataset.update_from_yaml()
                dataset.create_in_hdx()
                topline_indicators.extend(country_topline_indicators)
                country_isos.append(countryiso)

    dataset = generate_topline_dataset(folder, topline_indicators, country_isos)
    logger.info('Adding topline indicators')
    dataset.update_from_yaml(path=join('config', 'hdx_topline_dataset_static.yml'))
    dataset.create_in_hdx()

if __name__ == '__main__':
    facade(main, hdx_site='test', project_config_yaml=join('config', 'project_configuration.yml'))
