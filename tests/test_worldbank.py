#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for worldbank.

'''
from os.path import join

import pytest
from hdx.configuration import Configuration

from worldbank import generate_dataset, get_indicators_and_tags, get_countries


class TestWorldBank:
    indicators = [('AG.LND.TOTL.K2', 'Land area (sq. km)',
                               "Land area is a country's total area... and lakes.",
                               'Food and Agriculture Organization, electronic files and web site.')]

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True,
                             project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
    @pytest.fixture(scope='function')
    def downloader(self):
        class Request:
            def json(self):
                pass

        class Download:
            @staticmethod
            def download(url):
                request = Request()
                if url == 'http://lala/indicator/AG.LND.TOTL.K2?format=json&per_page=10000':
                    def fn():
                        return [{'pages': 1, 'per_page': '10000', 'page': 1, 'total': 1},
                                [{'topics': [{'value': 'Agriculture & Rural Development  ', 'id': '1'},
                                             {'value': 'Environment ', 'id': '6'}],
                                  'source': {'value': 'World Development Indicators', 'id': '2'},
                                  'sourceNote': "Land area is a country's total area... and lakes.",
                                  'sourceOrganization': 'Food and Agriculture Organization, electronic files and web site.',
                                  'id': 'AG.LND.TOTL.K2', 'name': 'Land area (sq. km)'}]]
                    request.json = fn
                elif url == 'http://haha/countries?format=json&per_page=10000':
                    def fn():
                        return [{'per_page': '10000', 'pages': 1, 'page': 1, 'total': 304},
                                [{'name': 'Aruba', 'region': {'value': 'Latin America & Caribbean ', 'id': 'LCN'},
                                  'capitalCity': 'Oranjestad', 'id': 'ABW',
                                  'lendingType': {'value': 'Not classified', 'id': 'LNX'},
                                  'adminregion': {'value': '', 'id': ''}, 'iso2Code': 'AW', 'longitude': '-70.0167',
                                  'incomeLevel': {'value': 'High income', 'id': 'HIC'}, 'latitude': '12.5167'},
                                 {'name': 'Afghanistan', 'region': {'value': 'South Asia', 'id': 'SAS'},
                                  'capitalCity': 'Kabul', 'id': 'AFG', 'lendingType': {'value': 'IDA', 'id': 'IDX'},
                                  'adminregion': {'value': 'South Asia', 'id': 'SAS'}, 'iso2Code': 'AF',
                                  'longitude': '69.1761', 'incomeLevel': {'value': 'Low income', 'id': 'LIC'},
                                  'latitude': '34.5228'},
                                 {'name': 'Africa', 'region': {'value': 'Aggregates', 'id': 'NA'},
                                  'capitalCity': '', 'id': 'AFR',
                                  'lendingType': {'value': 'Aggregates', 'id': ''},
                                  'adminregion': {'value': '', 'id': ''}, 'iso2Code': 'A9', 'longitude': '',
                                  'incomeLevel': {'value': 'Aggregates', 'id': 'NA'}, 'latitude': ''},
                                 {'name': 'Angola', 'region': {'value': 'Sub-Saharan Africa ', 'id': 'SSF'},
                                  'capitalCity': 'Luanda', 'id': 'AGO', 'lendingType': {'value': 'IBRD', 'id': 'IBD'},
                                  'adminregion': {'value': 'Sub-Saharan Africa (excluding high income)', 'id': 'SSA'},
                                  'iso2Code': 'AO', 'longitude': '13.242',
                                  'incomeLevel': {'value': 'Upper middle income', 'id': 'UMC'},
                                  'latitude': '-8.81155'}]]
                    request.json = fn
                return request
        return Download()

    def test_get_indicators_and_tags(self, downloader):
        indicators, tags = get_indicators_and_tags('http://lala/', downloader, ['AG.LND.TOTL.K2'])
        assert indicators == TestWorldBank.indicators
        assert tags == ['Agriculture', 'Rural Development', 'Environment']

    def test_get_countries(self, downloader):
        countries = get_countries('http://haha/', downloader)
        countries = list(countries)
        assert countries == [('ABW', 'Aruba'), ('AFG', 'Afghanistan'), ('AGO', 'Angola')]

    def test_generate_dataset(self, configuration):
        dataset = generate_dataset('AFG', 'Afghanistan', TestWorldBank.indicators)
        assert dataset == {'title': 'World Bank Indicators for Afghanistan', 'groups': [{'name': 'afg'}],
                           'data_update_frequency': '1', 'dataset_date': '06/27/2017',
                           'tags': [{'name': 'indicators'}, {'name': 'World Bank'}],
                           'name': 'world-bank-indicators-for-afghanistan'}
