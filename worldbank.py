#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
World Bank:
----------

Generates World Bank datasets.

"""
import csv
import json
import logging

import re
from collections import OrderedDict
from os.path import join

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.resource_view import ResourceView
from hdx.data.showcase import Showcase
from hdx.utilities.dictandlist import write_list_to_csv, dict_of_lists_add
from slugify import slugify

logger = logging.getLogger(__name__)
quickchart_resourceno = 0


def get_topics(base_url, downloader):
    url = '%sv2/en/source?format=json&per_page=10000' % base_url
    response = downloader.download(url)
    json = response.json()
    valid_sources = list()
    for source in json[1]:
        if source['dataavailability'] != 'Y':
            continue
        if 'archive' in source['name'].lower():
            continue
        valid_sources.append(source['id'])
    url = '%sv2/en/topic?format=json&per_page=10000' % base_url
    response = downloader.download(url)
    json = response.json()
    topics = json[1]
    for topic in topics:
        tags = list()
        tag_name = topic['value'].lower()
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
            source_id = indicator['source']['id']
            if source_id not in valid_sources:
                continue
            dict_of_lists_add(sources, indicator['source']['id'], indicator)
        topic['sources'] = sources
    return topics


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
    word_regexp = re.findall(r'\w+', indicator_name)
    if word_regexp:
        found_per = False
        for word in word_regexp:
            if word == 'per':
                found_per = True
        if found_per:
            return indicator_name
    if 'population' in indicator_name.lower():
        return 'people'
    if indicator_name[:9] == 'Number of':
        return indicator_name[10:]
    if ',' in indicator_name:
        return indicator_name[indicator_name.find(',')+1:].strip()
    logger.warning('Using full indicator name as unit: %s' % indicator_name)
    return indicator_name


def generate_dataset_and_showcase(base_url, downloader, folder, countryiso, countryiso2, countryname, topic,
                                  indicator_limit, character_limit, tag_mappings, topline_indicator_codes,
                                  topline_indicators):
    topicname = topic['value'].replace('&', 'and')
    title = '%s - %s' % (countryname, topicname)
    slugified_name = slugify('World Bank %s Indicators for %s' % (topicname, countryname)).lower()

    dataset = Dataset({
        'name': slugified_name,
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
    for i, tag in enumerate(tags):
        if tag in tag_mappings:
            tags[i] = tag_mappings[tag]
    tags.append('hxl')
    dataset.add_tags(tags)

    earliest_year = 10000
    latest_year = 0
    qc_indicators = [None, None, None]
    indicator_names_dict = dict()
    indicators_len_dict = dict()
    rows = list()

    def add_rows(jsondata):
        nonlocal earliest_year
        nonlocal latest_year
        for metadata in jsondata:
            value = metadata['value']
            if value is None:
                continue
            indicator_code = metadata['indicator']['id']
            indicator_name = metadata['indicator']['value']
            indicator_names_dict[indicator_code] = indicator_name
            year = int(metadata['date'])
            if year < earliest_year:
                earliest_year = year
            elif year > latest_year:
                latest_year = year
            rows.append({'Country Name': countryname, 'Country ISO3': countryiso, 'Year': year,
                         'Indicator Name': indicator_name, 'Indicator Code': indicator_code,
                         'Value': value})
            len_indicator_code = len(indicator_code)
            indicators_dict = indicators_len_dict.get(len_indicator_code, OrderedDict())
            indicator_dict = indicators_dict.get(indicator_code, dict())
            indicator_dict[year] = value
            indicators_dict[indicator_code] = indicator_dict
            indicators_len_dict[len_indicator_code] = indicators_dict
            if indicator_code in topline_indicator_codes:
                topline_indicator = topline_indicators.get(indicator_code)
                if topline_indicator is None:
                    topline_year = 0
                else:
                    topline_year = topline_indicator['year']
                if year > topline_year:
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
                    topline_indicators[indicator_code] = topline_indicator

    start_url = '%sv2/en/country/%s/indicator/' % (base_url, countryiso)
    for source_id in topic['sources']:
        indicator_list = topic['sources'][source_id]
        indicator_list_len = len(indicator_list)
        i = 0
        while i < indicator_list_len:
            ie = min(i + indicator_limit, indicator_list_len)
            indicators_string = ';'.join([x['id'] for x in indicator_list[i:ie]])
            if len(indicators_string) > character_limit:
                indicators_string = ';'.join([x['id'] for x in indicator_list[i:ie-5]])
                i -= 5
            url = '%s%s?source=%s&format=json&per_page=10000' % (start_url, indicators_string, source_id)
            response = downloader.download(url)
            json = response.json()
            if json[0]['total'] == 0:
                i += indicator_limit
                continue
            if json[0]['pages'] != 1:
                raise ValueError('Not expecting more than one page!')
            add_rows(json[1])
            i += indicator_limit

    if earliest_year == 10000:
        logger.exception('%s has no data!' % countryname)
        return None, None, None

    indicator_names = set()
    for indicator_name_long in indicator_names_dict.values():
        ind0 = re.sub(r'\s+', ' ', indicator_name_long)
        ind1, _, _ = ind0.partition(',')
        ind2, _, _ = ind1.partition('(')
        indicator_name, _, _ = ind2.partition(':')
        indicator_names.add((indicator_name.strip()))
    dataset['notes'] = "%s\n\nContains data from the World Bank's [data portal](http://data.worldbank.org/).\n\nIndicators: %s" % (topic['sourceNote'], ', '.join(sorted(indicator_names)))

    for len_indicator_code in sorted(indicators_len_dict):
        indicators_dict = indicators_len_dict[len_indicator_code]
        for indicator_code in indicators_dict:
            ind_year_values = indicators_dict[indicator_code]
            if len(set(ind_year_values.values())) == 1:
                continue
            indicator_name = indicator_names_dict[indicator_code]
            if qc_indicators[0] is None:
                qc_indicators[0] = {'code': indicator_code, 'name': indicator_name}
            elif qc_indicators[1] is None:
                qc_indicators[1] = {'code': indicator_code, 'name': indicator_name}
            elif qc_indicators[2] is None:
                qc_indicators[2] = {'code': indicator_code, 'name': indicator_name}

    headers = ['Country Name', 'Country ISO3', 'Year', 'Indicator Name', 'Indicator Code', 'Value']
    hxlrow = {'Country Name': '#country+name', 'Country ISO3': '#country+code', 'Year': '#date+year',
              'Indicator Name': '#indicator+name', 'Indicator Code': '#indicator+code', 'Value': '#indicator+value+num'}

    rows.insert(0, hxlrow)
    slug_topicname = slugify(topicname)
    filepath = join(folder, '%s_%s.csv' % (slug_topicname, countryiso))
    write_list_to_csv(rows, filepath, headers=headers)

    resource_data = {
        'name': topicname,
        'description': 'HXLated csv containing all the %s indicators' % topicname,
    }
    resource = Resource(resource_data)
    resource.set_file_type('csv')
    resource.set_file_to_upload(filepath)
    dataset.add_update_resource(resource)

    dataset.set_dataset_year_range(earliest_year, latest_year)
    dataset.set_quickchart_resource(quickchart_resourceno)

    iso2upper = countryiso2.upper()
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': '%s indicators for %s' % (topicname, countryname),
        'notes': '%s indicators for %s' % (topicname, countryname),
        'url': 'https://data.worldbank.org/topic/%s?locations=%s' % (slug_topicname, iso2upper),
        'image_url': 'https://www.worldbank.org/content/dam/wbr/logo/logo-wb-header-en.svg'
    })
    showcase.add_tags(tags)
    return dataset, showcase, qc_indicators


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


def generate_resource_view(dataset, indicators):
    resourceview = ResourceView({'resource_id': dataset.get_resource(quickchart_resourceno)['id']})
    resourceview.update_from_yaml()
    hxl_preview_config = resourceview['hxl_preview_config']
    if indicators[0] is not None:
        hxl_preview_config = hxl_preview_config.replace('AG.AGR.TRAC.NO', indicators[0]['code'])
        hxl_preview_config = hxl_preview_config.replace('Chart 1', indicators[0]['name'])
        hxl_preview_config = hxl_preview_config.replace('Value 1', get_unit(indicators[0]['name']))
        if indicators[1] is not None:
            hxl_preview_config = hxl_preview_config.replace('AG.CON.FERT.PT.ZS', indicators[1]['code'])
            hxl_preview_config = hxl_preview_config.replace('Chart 2', indicators[1]['name'])
            hxl_preview_config = hxl_preview_config.replace('Value 2', get_unit(indicators[1]['name']))
            if indicators[2] is not None:
                hxl_preview_config = hxl_preview_config.replace('AG.CON.FERT.ZS', indicators[2]['code'])
                hxl_preview_config = hxl_preview_config.replace('Chart 3', indicators[2]['name'])
                hxl_preview_config = hxl_preview_config.replace('Value 3', get_unit(indicators[2]['name']))
            else:
                hxl_preview_config = json.loads(hxl_preview_config)
                del hxl_preview_config['bites'][2]
                hxl_preview_config = json.dumps(hxl_preview_config)
        else:
            hxl_preview_config = json.loads(hxl_preview_config)
            del hxl_preview_config['bites'][2]
            del hxl_preview_config['bites'][1]
            hxl_preview_config = json.dumps(hxl_preview_config)
        resourceview['hxl_preview_config'] = hxl_preview_config
    else:
        resourceview = None
    return resourceview

