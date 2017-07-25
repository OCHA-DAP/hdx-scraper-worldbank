#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
World Bank:
----------

Generates World Bank datasets.

"""
import logging

import re
from json import dump
from os.path import join

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


def get_unit(indicator_name):
    unit_regexp = re.search(r'\((.*?)\)', indicator_name)
    if unit_regexp:
        return unit_regexp.group(1)
    if 'population' in indicator_name.lower():
        return 'people'
    raise HDXError('No unit for Unrecognised indicator %s' % indicator_name)


def generate_dataset(base_url, downloader, folder, countryiso, countryname, indicators, topline_indicators):
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
    dataset.set_expected_update_frequency('Every year')
    dataset.add_tags(['indicators', 'World Bank'])

    earliest_year = 10000
    latest_year = 0
    indicators_json = list()
    for indicator_code, indicator_name, indicator_note, indicator_source in indicators:
        url = '%scountries/%s/indicators/%s?format=json&per_page=10000' % (base_url, countryiso, indicator_code)
        response = downloader.download(url)
        json = response.json()
        indicator_dict = dict()
        for yeardict in json[1]:
            year = int(yeardict['date'])
            indicator_dict[year] = yeardict
        years = sorted(indicator_dict.keys())
        indicator_earliest_year = years[0]
        indicator_latest_year = years[-1]
        if indicator_latest_year > latest_year:
            latest_year = indicator_latest_year
        if indicator_earliest_year < earliest_year:
            earliest_year = indicator_earliest_year
        if indicator_code in topline_indicators:
            indicator_year_index = len(years)
            year = None
            value = None
            while indicator_year_index != 0:
                indicator_year_index -= 1
                year = years[indicator_year_index]
                value = indicator_dict[year]['value']
                if value is not None:
                    break
            if value is None:
                continue
            unit = get_unit(indicator_name)
            indicator_json = {
                'value': value,
                'indicatorTypeCode': indicator_code,
                'indicatorTypeName': indicator_name,
                'unitCode': unit,
                'unitName': unit,
                'locationCode': countryiso.upper(),
                'locationName': countryname.upper(),
                'sourceCode': 'world-bank',
                'sourceName': 'World Bank',
                'time': '%d-01-01' % year
            }
            indicators_json.append(indicator_json)

        resource_data = {
            'name': indicator_name,
            'description': 'Source: %s  \n   \n%s' % (indicator_source, indicator_note),
            'format': 'json',
            'url': url
        }
        resource = Resource(resource_data)
        dataset.add_update_resource(resource)
    dataset.set_dataset_year_range(earliest_year, latest_year)

    topline_json = {
        'success': True,
        'errorMessage': 'None',
        'totalCount': len(indicators_json),
        'currentPage': None,
        'totalNumOfPages': None,
        'pageSize': None,
        'moreResults': None,
        'results': indicators_json
    }

    filepath = join(folder, 'worldbank_topline_%s.json' % countryiso)
    with open(filepath, 'w') as outfile:
        dump(topline_json, outfile)

    resource_data = {
        'name': 'topline',
        'description': 'File for country topline numbers',
        'format': 'json'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(filepath)
    dataset.add_update_resource(resource)

    return dataset
