#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
World Bank:
----------

Generates World Bank datasets.

"""
import csv
import logging

import re
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


def generate_dataset(base_url, downloader, countryiso, countryname, indicators, topline_indicator_codes):
    """
    http://api.worldbank.org/countries/bra/indicators/NY.GNP.PCAP.CD
    """
    title = 'Economic and Social Indicators'
    slugified_name = slugify('World Bank Indicators for %s' % countryname).lower()

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('hdx')
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None
    dataset.set_expected_update_frequency('Every year')
    dataset.add_tags(['indicators', 'World Bank'])

    earliest_year = 10000
    latest_year = 0
    topline_indicators = list()
    for indicator_code, indicator_name, indicator_note, indicator_source in indicators:
        url = '%scountries/%s/indicators/%s?format=json&per_page=10000' % (base_url, countryiso, indicator_code)
        response = downloader.download(url)
        json = response.json()
        indicator_dict = dict()
        result = json[1]
        if result is None:
            continue
        for yeardict in result:
            year = int(yeardict['date'])
            indicator_dict[year] = yeardict
        years = sorted(indicator_dict.keys())
        indicator_earliest_year = years[0]
        indicator_latest_year = years[-1]
        if indicator_latest_year > latest_year:
            latest_year = indicator_latest_year
        if indicator_earliest_year < earliest_year:
            earliest_year = indicator_earliest_year
        if indicator_code in topline_indicator_codes:
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
            topline_indicator_name = indicator_name.replace(' (%s)' % unit, '')
            topline_indicator = {
                'countryiso': countryiso.upper(),
                'indicator': topline_indicator_name,
                'source': 'World Bank',
                'url': url,
                'year': year,
                'unit': get_unit(indicator_name),
                'value': value
            }
            topline_indicators.append(topline_indicator)

        resource_data = {
            'name': indicator_name,
            'description': 'Source: %s  \n   \n%s' % (indicator_source, indicator_note),
            'format': 'json',
            'url': url
        }
        resource = Resource(resource_data)
        dataset.add_update_resource(resource)

    if earliest_year == 10000:
        logger.exception('%s has no data!' % countryname)
        return None, None

    dataset.set_dataset_year_range(earliest_year, latest_year)
    return dataset, topline_indicators


def generate_topline_dataset(folder, topline_indicators, country_isos):
    title = 'Topline Indicators'
    slugified_name = slugify('World Bank Country Topline Indicators').lower()

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('hdx')
    dataset.add_country_locations(country_isos)
    dataset.set_expected_update_frequency('Every year')

    earliest_year = 10000
    latest_year = 0
    for topline_indicator in topline_indicators:
        year = topline_indicator['year']
        if year > latest_year:
            latest_year = year
        if year < earliest_year:
            earliest_year = year
    dataset.set_dataset_year_range(earliest_year, latest_year)

    dataset.add_tags(['indicators', 'World Bank'])
    filepath = join(folder, 'worldbank_topline.csv')
    hxl = {
        'countryiso': '#country+code',
        'indicator': '#indicator+name',
        'source': '#meta+source',
        'url': '#meta+url',
        'date': '#date',
        'unit': '#indicator+unit',
        'value': '#value+amount'
    }
    with open(filepath, 'w') as csvfile:
        fieldnames = ['countryiso', 'indicator', 'source', 'url', 'date', 'unit', 'value']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(hxl)
        for topline_indicator in topline_indicators:
            topline_indicator['date'] = '%d-01-01' % topline_indicator['year']
            del topline_indicator['year']
            writer.writerow(topline_indicator)

    resource_data = {
        'name': 'topline_indicators',
        'description': 'Country topline indicators',
        'format': 'csv'
    }
    resource = Resource(resource_data)
    resource.set_file_to_upload(filepath)
    dataset.add_update_resource(resource)

    return dataset
