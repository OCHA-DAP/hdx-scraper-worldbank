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
headers = ['Country Name', 'Country ISO3', 'Year', 'Indicator Name', 'Indicator Code', 'Value']
hxlrow = {'Country Name': '#country+name', 'Country ISO3': '#country+code', 'Year': '#date+year',
          'Indicator Name': '#indicator+name', 'Indicator Code': '#indicator+code', 'Value': '#indicator+value+num'}
resource_name = '%s Indicators for %s'


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
        value = topic['value']
        tag_name = value.lower()
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
        topic['value'] = value.replace('&', 'and')
    return topics


def get_countries(base_url, downloader):
    url = '%sv2/en/country?format=json&per_page=10000' % base_url
    response = downloader.download(url)
    json = response.json()
    for country in json[1]:
        if country['region']['value'] != 'Aggregates':
            yield {'name': country['name'], 'iso3': country['id'], 'iso2': country['iso2Code']}


def get_unit(indicator_name):
    if indicator_name[:9] == 'Coverage:':
        return 'Coverage Rate'
    word_regexp = re.findall(r'\w+', indicator_name)
    found_per = False
    if word_regexp:
        for word in word_regexp:
            if word == 'per':
                found_per = True
    unit_regexp = re.findall(r'\((.*?)\)', indicator_name)
    if unit_regexp:
        result = (' '.join(unit_regexp)).strip()
        findrangeonly = re.search(r'^[0-9]+-[0-9]+$', result)
        if findrangeonly is None:
            findvalonly = re.search(r'^[0-9]+$', result)
            if findvalonly is None:
                if not (found_per and 'per' not in result):
                    finddollarval = re.search(r'\$[0-9]+', result)
                    if finddollarval is None:
                        return result
    if found_per:
        if indicator_name[:9] == 'Number of':
            return indicator_name[10:].strip()
        return indicator_name.strip()
    if 'percentage' in indicator_name.lower():
        return '%'
    if 'population' in indicator_name.lower():
        return 'people'
    if indicator_name[:9] == 'Number of':
        return indicator_name[10:].strip()
    if ',' in indicator_name:
        return indicator_name[indicator_name.find(',')+1:].strip()
    logger.warning('Using full indicator name as unit: %s' % indicator_name)
    return indicator_name.strip()


def get_topic_dataset_name(topicname, countryname):
    return slugify('World Bank %s Indicators for %s' % (topicname, countryname)).lower()


def get_combined_dataset_name(countryname):
    return slugify('World Bank Combined Indicators for %s' % countryname).lower()


def generate_dataset_and_showcase(site_url, configuration, downloader, folder, country, topic, topline_indicators):
    countryname = country['name']
    topicname = topic['value']
    title = '%s - %s' % (countryname, topicname)
    slugified_name = get_topic_dataset_name(topicname, countryname)

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('hdx')
    dataset.set_subnational(False)
    countryiso = country['iso3']
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None, None, None, None, topicname
    dataset.set_expected_update_frequency('Every year')

    tag_mappings = configuration['tag_mappings']
    tags = topic['tags']
    for i, tag in enumerate(tags):
        if tag in tag_mappings:
            tags[i] = tag_mappings[tag]
    tags.append('hxl')
    tags.append('indicators')
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
            if indicator_code in configuration['topline_indicators']:
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
                        'unit': unit,
                        'value': value
                    }
                    topline_indicators[indicator_code] = topline_indicator

    base_url = configuration['base_url']
    indicator_limit = configuration['indicator_limit']
    character_limit = configuration['character_limit']
    start_url = '%sv2/en/country/%s/indicator/' % (base_url, countryiso)
    for source_id in topic['sources']:
        indicator_list = topic['sources'][source_id]
        indicator_list_len = len(indicator_list)
        i = 0
        while i < indicator_list_len:
            ie = min(i + indicator_limit, indicator_list_len)
            indicators_string = ';'.join([x['id'] for x in indicator_list[i:ie]])
            if len(indicators_string) > character_limit:
                ie -= configuration['indicator_subtract']
                indicators_string = ';'.join([x['id'] for x in indicator_list[i:ie]])
                i = ie - indicator_limit
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
        logger.error('%s has no data!' % title)
        return None, None, None, None, None, topicname

    indicator_names = set()
    for indicator_name_long in indicator_names_dict.values():
        ind0 = re.sub(r'\s+', ' ', indicator_name_long)
        ind1, _, _ = ind0.partition(',')
        ind2, _, _ = ind1.partition('(')
        indicator_name, _, _ = ind2.partition(':')
        indicator_names.add((indicator_name.strip()))
    comb_dsname = get_combined_dataset_name(countryname)
    notes = ["Contains data from the World Bank's [data portal](http://data.worldbank.org/). ",
             'There is also a [consolidated country dataset](%s/dataset/%s) on HDX.\n\n' % (site_url, comb_dsname),
             topic['sourceNote'], '\n\nIndicators: %s' % ', '.join(sorted(indicator_names))]
    dataset['notes'] = ''.join(notes)

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

    rows.insert(0, hxlrow)
    slug_topicname = slugify(topicname)
    filepath = join(folder, '%s_%s.csv' % (slug_topicname, countryiso))
    write_list_to_csv(rows, filepath, headers=headers)

    resource_data = {
        'name': resource_name % (topicname, countryname),
        'description': 'HXLated csv containing %s indicators' % topicname,
    }
    resource = Resource(resource_data)
    resource.set_file_type('csv')
    resource.set_file_to_upload(filepath)
    dataset.add_update_resource(resource)

    dataset.set_dataset_year_range(earliest_year, latest_year)
    dataset.set_quickchart_resource(quickchart_resourceno)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': '%s indicators for %s' % (topicname, countryname),
        'notes': '%s indicators for %s' % (topicname, countryname),
        'url': 'https://data.worldbank.org/topic/%s?locations=%s' % (slug_topicname, country['iso2'].upper()),
        'image_url': 'https://www.worldbank.org/content/dam/wbr/logo/logo-wb-header-en.svg'
    })
    showcase.add_tags(tags)
    return dataset, showcase, qc_indicators, earliest_year, latest_year, rows


def generate_combined_dataset_and_showcase(site_url, folder, country, tags, topics, ignore_topics, earliest_years,
                                           latest_years, rows):
    indicators = 'Economic, Social, Environmental, Health, Education, Development and Energy'
    countryname = country['name']
    title = '%s - %s' % (countryname, indicators)
    slugified_name = get_combined_dataset_name(countryname)

    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('hdx')
    dataset.set_subnational(False)
    countryiso = country['iso3']
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None
    dataset.set_expected_update_frequency('Every year')
    dataset.add_tags(tags)
    topiclist = list()
    for topic in topics:
        topicname = topic['value']
        if topicname in ignore_topics:
            continue
        topic_dataset_name = get_topic_dataset_name(topicname, countryname)
        topiclist.append('[%s](%s/dataset/%s)' % (topicname, site_url, topic_dataset_name))
    notes = ["Contains data from the World Bank's [data portal](http://data.worldbank.org/) covering the ",
             "following topics which also exist as individual datasets on HDX: %s." % ', '.join(topiclist)]
    dataset['notes'] = ''.join(notes)
    earliest_year = sorted(earliest_years)[0]
    latest_year = sorted(latest_years)[-1]
    dataset.set_dataset_year_range(earliest_year, latest_year)
    cutdownrows = list()
    for row in rows:
        if row['Indicator Code'] in ['SP.POP.TOTL', 'SP.DYN.LE00.IN', 'SE.PRM.ENRR']:
            cutdownrows.append(row)
    rows.insert(0, hxlrow)
    cutdownrows.insert(0, hxlrow)
    filepath = join(folder, 'indicators_%s.csv' % countryiso)
    write_list_to_csv(rows, filepath, headers=headers)
    resource_data = {
        'name': resource_name % ('Combined', countryname),
        'description': 'HXLated csv containing %s indicators' % indicators,
    }
    resource = Resource(resource_data)
    resource.set_file_type('csv')
    resource.set_file_to_upload(filepath)
    dataset.add_update_resource(resource)

    filepath = join(folder, 'qc_indicators_%s.csv' % countryiso)
    write_list_to_csv(cutdownrows, filepath, headers=headers)
    resource_data = {
        'name': 'QuickCharts %s' % resource_name % ('Combined', countryname),
        'description': 'QuickCharts resource',
    }
    resource = Resource(resource_data)
    resource.set_file_type('csv')
    resource.set_file_to_upload(filepath)
    dataset.add_update_resource(resource)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': 'Indicators for %s' % countryname,
        'notes': '%s indicators for %s' % (indicators, countryname),
        'url': 'https://data.worldbank.org/?locations=%s' % country['iso2'].upper(),
        'image_url': 'https://www.worldbank.org/content/dam/wbr/logo/logo-wb-header-en.svg'
    })
    showcase.add_tags(tags)

    return dataset, showcase


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
        hxl_preview_config = hxl_preview_config.replace('SP.POP.TOTL', indicators[0]['code'])
        hxl_preview_config = hxl_preview_config.replace('Total Population', indicators[0]['name'])
        hxl_preview_config = hxl_preview_config.replace('People', get_unit(indicators[0]['name']))
        if indicators[1] is not None:
            hxl_preview_config = hxl_preview_config.replace('SP.DYN.LE00.IN', indicators[1]['code'])
            hxl_preview_config = hxl_preview_config.replace('Life Expectancy at Birth', indicators[1]['name'])
            hxl_preview_config = hxl_preview_config.replace('Years', get_unit(indicators[1]['name']))
            if indicators[2] is not None:
                hxl_preview_config = hxl_preview_config.replace('SE.PRM.ENRR', indicators[2]['code'])
                hxl_preview_config = hxl_preview_config.replace('Primary School Enrollment', indicators[2]['name'])
                hxl_preview_config = hxl_preview_config.replace('Gross Percentage', get_unit(indicators[2]['name']))
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


def generate_all_datasets_showcases(configuration, downloader, folder, country, topics, country_isos,
                                    topline_indicators, create_dataset_showcase):
    site_url = configuration.get_hdx_site_url()

    allrows = list()
    alltags = set()
    earliest_years = set()
    latest_years = set()
    topline_indicator_dict = dict()
    ignore_topics = list()
    for topic in topics:
        dataset, showcase, qc_indicators, earliest_year, latest_year, rows = \
            generate_dataset_and_showcase(site_url, configuration, downloader, folder, country, topic, topline_indicator_dict)
        if dataset is None:
            ignore_topics.append(rows)
        else:
            logger.info('Adding %s %s' % (country['name'], topic['value']))
            allrows.extend(rows[1:])
            alltags.update(dataset.get_tags())
            earliest_years.add(earliest_year)
            latest_years.add(latest_year)
            country_isos.append(country['iso3'])
            create_dataset_showcase(dataset, showcase, qc_indicators)
    if len(ignore_topics) == len(topics):
        return None, None
    topline_indicators.extend(topline_indicator_dict.values())
    return generate_combined_dataset_and_showcase(site_url, folder, country, sorted(alltags), topics, ignore_topics,
                                                  earliest_years, latest_years, allrows)

