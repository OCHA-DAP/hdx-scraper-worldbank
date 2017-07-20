#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
World Bank:
----------

Generates World Bank datasets.

"""
import logging

from hdx.hdx_configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from slugify import slugify


logger = logging.getLogger(__name__)


def get_indicators_and_tags(base_url, downloader, indicator_list):
    indicators = list()
    tags = list()
    for indicator in indicator_list:
        url = '%sindicator/%s?format=json&per_page=10000' % (base_url, indicator)
        response = downloader.download(url)
        json = response.json()
        result = json[1][0]
        for tag in result['topics']:
            tag_name = tag['value']
            if '&' in tag_name:
                tag_names = tag_name.split(' & ')
                for tag_name in tag_names:
                    tags.append(tag_name.strip())
            else:
                tags.append(tag_name.strip())
        indicators.append((indicator, result['name'], result['sourceNote'], result['sourceOrganization']))
    return indicators, tags


def get_countries(base_url, downloader):
    url = '%scountries?format=json&per_page=10000' % base_url
    response = downloader.download(url)
    json = response.json()
    for country in json[1]:
        if country['region']['value'] != 'Aggregates':
            yield country['id'], country['name']


def get_dataset_date_range(base_url, downloader):
    url = '%scountries/USA/indicators/SP.POP.TOTL?format=json&per_page=10000' % base_url
    response = downloader.download(url)
    json = response.json()
    years = list()
    for yeardict in json[1]:
        years.append(yeardict['date'])
    years = sorted(years)
    return years[0], years[-1]


def generate_dataset(base_url, countryiso, countryname, indicators, dataset_date_range):
    """
    http://api.worldbank.org/countries/bra/indicators/NY.GNP.PCAP.CD
    """
    title = 'World Bank Indicators for %s' % countryname
    slugified_name = slugify(title).lower()

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None
    dataset.set_dataset_year_range(dataset_date_range[0], dataset_date_range[1])
    dataset.set_expected_update_frequency('Every day')
    dataset.add_tags(['indicators', 'World Bank'])

    for indicator_code, indicator_name, indicator_note, indicator_source in indicators:
        url = '%scountries/%s/indicators/%s?format=json&per_page=10000' % (base_url, countryiso, indicator_code)
        resource_data = {
            'name': indicator_name,
            'description': 'Source: %s  \n   \n%s' % (indicator_source, indicator_note),
            'format': 'json',
            'url': url
        }
        resource = Resource(resource_data)
        dataset.add_update_resource(resource)

    return dataset
