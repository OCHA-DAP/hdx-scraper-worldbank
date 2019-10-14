#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from worldbank import get_countries, get_topics, generate_dataset_and_showcase, generate_topline_dataset, \
    generate_resource_view

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-worldbank'


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    with temp_dir('worldbank') as folder:
        with Download(status_forcelist=[400, 429, 500, 502, 503, 504]) as downloader:
            indicator_limit = Configuration.read()['indicator_limit']
            character_limit = Configuration.read()['character_limit']
            tag_mappings = Configuration.read()['tag_mappings']
            topline_indicator_names = Configuration.read()['topline_indicators']
            country_isos = list()
            topline_indicators = list()
            topics = get_topics(base_url, downloader)
            for countryiso, countryiso2, countryname in get_countries(base_url, downloader):
                topline_indicators_dict = dict()
                for topic in topics:
                    dataset, showcase, qc_indicators = \
                        generate_dataset_and_showcase(base_url, downloader, folder, countryiso, countryiso2, countryname,
                                                      topic, indicator_limit, character_limit, tag_mappings,
                                                      topline_indicator_names, topline_indicators_dict)
                    if dataset is not None:
                        logger.info('Adding %s' % countryname)
                        dataset.update_from_yaml()
                        dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False)
                        resource_view = generate_resource_view(dataset, qc_indicators)
                        if resource_view:
                            resource_view.create_in_hdx()
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)
                        country_isos.append(countryiso)
                topline_indicators.extend(topline_indicators_dict.values())

        dataset = generate_topline_dataset(folder, topline_indicators, country_isos)
        logger.info('Adding topline indicators')
        dataset.update_from_yaml(path=join('config', 'hdx_topline_dataset_static.yml'))
        dataset.create_in_hdx()


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
