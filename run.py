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

from worldbank import get_countries, get_topics, generate_dataset_and_showcase, generate_topline_dataset

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-worldbank'


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    with temp_dir('worldbank') as folder:
        with Download(status_forcelist=[400, 429, 500, 502, 503, 504]) as downloader:
            topline_indicator_names = Configuration.read()['topline_indicators']
            country_isos = list()
            topline_indicators = list()
            for countryiso, countryiso2, countryname in get_countries(base_url, downloader):
                if countryiso != 'AFG':  # Remove!
                    continue
                for topic in get_topics(base_url, downloader):
                    dataset, showcase, dataset_topline_indicators = \
                        generate_dataset_and_showcase(base_url, downloader, folder, countryiso, countryiso2, countryname,
                                                      topic, topline_indicator_names)
                    if dataset is not None:
                        logger.info('Adding %s' % countryname)
                        dataset.update_from_yaml()
                        dataset.create_in_hdx(remove_additional_resources=True)
                        resources = dataset.get_resources()
                        if resources[0].get_file_type() != 'CSV':
                            resource_ids = list()
                            for resource in resources:
                                resource_id = resource['id']
                                if resource.get_file_type() == 'CSV':
                                    resource_ids.insert(0, resource_id)
                                else:
                                    resource_ids.append(resource_id)
                            dataset.reorder_resources(resource_ids)
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)
                        topline_indicators.extend(dataset_topline_indicators.values())
                        country_isos.append(countryiso)

        dataset = generate_topline_dataset(folder, topline_indicators, country_isos)
        logger.info('Adding topline indicators')
        dataset.update_from_yaml(path=join('config', 'hdx_topline_dataset_static.yml'))
        dataset.create_in_hdx()


if __name__ == '__main__':
    facade(main, hdx_key='a4b67b0e-5a89-469c-b29b-4c9b51ab9311', user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
