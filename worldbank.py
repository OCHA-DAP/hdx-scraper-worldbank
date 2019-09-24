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
from itertools import zip_longest
from os.path import join

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.utilities.dictandlist import write_list_to_csv, dict_of_lists_add
from slugify import slugify


logger = logging.getLogger(__name__)


def get_topics_metadata(base_url, downloader):
    url = '%sv2/en/topic?format=json&per_page=10000' % base_url
    response = downloader.download(url)
    json = response.json()
    topics = json[1]
    for topic in topics:
        tags = list()
        tag_name = topic['value']
        if '&' in tag_name:
            tag_names = tag_name.split(' & ')
            for tag_name in tag_names:
                tags.append(tag_name.strip())
        else:
            tags.append(tag_name.strip())
        topic['tags'] = tags
        url = '%sv2/en/topic/%s/indicator?format=json&per_page=10000' % (base_url, topic['id'])
        response = downloader.download(url)
        json = response.json()
        sources = dict()
        for indicator in json[1]:
            dict_of_lists_add(sources, indicator['source']['id'], indicator)
        topic['sources'] = sources
    return topics


def get_topic(base_url, downloader, topic):
    url = '%sv2/en/topic/%s?downloadformat=csv' % (base_url, topic['id'])
    response = downloader.download(url)


def get_countries(base_url, downloader):
    url = '%sv2/en/country?format=json&per_page=10000' % base_url
    response = downloader.download(url)
    json = response.json()
    for country in json[1]:
        if country['region']['value'] != 'Aggregates':
            yield country['id'], country['iso2Code'], country['name']


def get_unit(indicator_name):
    unit_regexp = re.search(r'\((.*?)\)', indicator_name)
    if unit_regexp:
        return unit_regexp.group(1)
    if 'population' in indicator_name.lower():
        return 'people'
    raise HDXError('No unit for Unrecognised indicator %s' % indicator_name)


def generate_dataset_and_showcase(base_url, downloader, folder, countryiso, countryiso2, countryname, topic,
                                  topline_indicator_codes):
    """
    """
    topicname = topic['value']
    title = '%s - %s' % (countryname, topicname)
    slugified_name = slugify('World Bank %s Indicators for %s' % (topicname, countryname)).lower()

    dataset = Dataset({
        'name': slugified_name,
        'notes': "%s\n\nContains data from the World Bank's [data portal](http://data.worldbank.org/)." % topic['sourceNote'],
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('hdx')
    dataset.set_subnational(False)
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None, None
    dataset.set_expected_update_frequency('Every year')
    tags = topic['tags']
    dataset.add_tags(tags)

    earliest_year = 10000
    latest_year = 0
    topline_indicators = list()
    rows = list()

    def add_rows(jsondata):
        for metadata in jsondata:
            value = metadata['value']
            if value is None:
                continue
            indicator_code = metadata['indicator']['id']
            indicator_name = metadata['indicator']['value']
            year = int(metadata['date'])
            rows.append({'Country Name': countryname, 'Country ISO3': countryiso, 'Year': year,
                         'Indicator Name': indicator_name, 'Indicator Code': indicator_code,
                         'Value': value})
    'http://api.worldbank.org/v2/en/en/topic/1?downloadformat=csv'
    start_url = '%sv2/en/country/%s/indicator/' % (base_url, countryiso)
    for source_id in topic['sources']:
        indicator_list = topic['sources'][source_id]
        indicator_list_len = len(indicator_list)
        for i in range(0, indicator_list_len, 100):
            ie = min(i + 100, indicator_list_len)
            indicators_string = ';'.join([x['id'] for x in indicator_list[i:ie]])
            url = '%s%s?source=%s&format=json&per_page=10000' % (start_url, indicators_string, source_id)
            response = downloader.download(url)
            json = response.json()
            total = json[0]['total']
            add_rows(json[1])
            for page in range(2, total):
                url = '%s%s?source=%s&format=json&per_page=10000&page=%d' % (start_url, indicators_string, source_id, page)
                response = downloader.download(url)
                json = response.json()
                add_rows(json[1])

    url = '%sv2/en/sources/2/series/all/country/%s?format=json&per_page=10000' % (base_url, countryiso)
    response = downloader.download(url)
    json = response.json()


    for indicator_code, indicator_name, indicator_note, indicator_source in indicators:
        url = '%scountries/%s/indicators/%s?format=json&per_page=10000' % (base_url, countryiso, indicator_code)
        response = downloader.download(url)
        json = response.json()
        indicator_dict = dict()
        result = json[1]
        if result is None:
            continue
        for jsonrow in result:
            year = int(jsonrow['date'])
            indicator_dict[year] = jsonrow
            rows.append({'Country Name': countryname, 'Country ISO3': countryiso, 'Year': year,
                         'Indicator Name': indicator_name, 'Indicator Code': indicator_code,
                         'Value': jsonrow['value']})
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
            'name': '%s' % indicator_name,
            'description': 'From API. Source: %s  \n   \n%s' % (indicator_source, indicator_note),
            'url': url
        }
        resource = Resource(resource_data)
        resource.set_file_type('json')
        dataset.add_update_resource(resource)

    headers = ['Country Name', 'Country ISO3', 'Year', 'Indicator Name', 'Indicator Code', 'Value']
    hxlrow = {'Country Name': '#country+name', 'Country ISO3': '#country+code', 'Year': '#date+year',
              'Indicator Name': '#indicator+name', 'Indicator Code': '#indicator+code', 'Value': '#indicator+num'}
    rows.insert(0, hxlrow)
    filepath = join(folder, 'all_indicators_%s.csv' % countryiso)
    write_list_to_csv(rows, filepath, headers=headers)

    resource_data = {
        'name': 'All Indicators',
        'description': 'HXLated csv containing all the indicators in each JSON resource below',
    }
    resource = Resource(resource_data)
    resource.set_file_type('csv')
    resource.set_file_to_upload(filepath)
    dataset.add_update_resource(resource)

    if earliest_year == 10000:
        logger.exception('%s has no data!' % countryname)
        return None, None, None

    dataset.set_dataset_year_range(earliest_year, latest_year)

    iso2lower = countryiso2.lower()
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'Indicators for %s' % countryname,
        'notes': 'Economic and social indicators for %s' % countryname,
        'url': 'https://data.worldbank.org/country/%s' % iso2lower,
        'image_url': 'http://databank.worldbank.org/data/download/site-content/wdi/maps/2017/world-by-income-wdi-2017.png'
    })
    showcase.add_tags(tags)
    return dataset, showcase, topline_indicators


def generate_topline_dataset(folder, topline_indicators, country_isos):
    title = 'Topline Indicators'
    slugified_name = slugify('World Bank Country Topline Indicators').lower()

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('hdx')
    dataset.set_subnational(False)
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

    dataset.add_tags(['indicators'])
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
