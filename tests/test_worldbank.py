#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for worldbank.

"""
from os.path import join
from tempfile import gettempdir

import pytest
from hdx.hdx_configuration import Configuration

from worldbank import generate_dataset, get_indicators_and_tags, get_countries, generate_topline_dataset


class TestWorldBank:
    indicators = [('AG.LND.TOTL.K2', 'Land area (sq. km)',
                               "Land area is a country's total area... and lakes.",
                               'Food and Agriculture Organization, electronic files and web site.')]
    topline_indicators = [{'indicator': 'Land area', 'source': 'World Bank', 'unit': 'sq. km', 'countryiso': 'AFG',
                           'year': 2016, 'value': '652860'}]
    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True,
                             project_config_yaml=join('tests', 'config', 'project_configuration.yml'))

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            def json(self):
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://lala/indicator/AG.LND.TOTL.K2?format=json&per_page=10000':
                    def fn():
                        return [{'pages': 1, 'per_page': '10000', 'page': 1, 'total': 1},
                                [{'topics': [{'value': 'Agriculture & Rural Development  ', 'id': '1'},
                                             {'value': 'Environment ', 'id': '6'}],
                                  'source': {'value': 'World Development Indicators', 'id': '2'},
                                  'sourceNote': "Land area is a country's total area... and lakes.",
                                  'sourceOrganization': 'Food and Agriculture Organization, electronic files and web site.',
                                  'id': 'AG.LND.TOTL.K2', 'name': 'Land area (sq. km)'}]]
                    response.json = fn
                elif url == 'http://papa/countries/AFG/indicators/AG.LND.TOTL.K2?format=json&per_page=10000':
                    def fn():
                        return [{'pages': 1, 'page': 1, 'per_page': '10000', 'total': 57},
                                [{'country': {'id': 'AF', 'value': 'Afghanistan'}, 'date': '2016', 'decimal': '1',
                                  'value': '652860', 'indicator': {'id': 'AG.LND.TOTL.K2',
                                                                   'value': 'Land area (sq. km)'}},
                                 {'country': {'id': 'AF', 'value': 'Afghanistan'}, 'date': '2015', 'decimal': '1',
                                  'value': '652860', 'indicator': {'id': 'AG.LND.TOTL.K2',
                                                                   'value': 'Land area (sq. km)'}},
                                 {'country': {'id': 'AF', 'value': 'Afghanistan'}, 'date': '2014', 'decimal': '1',
                                  'value': '652860', 'indicator': {'id': 'AG.LND.TOTL.K2',
                                                                   'value': 'Land area (sq. km)'}}]]

                    response.json = fn
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
                    response.json = fn
                return response
        return Download()

    def test_get_indicators_and_tags(self, downloader):
        indicators, tags = get_indicators_and_tags('http://lala/', downloader, ['AG.LND.TOTL.K2'])
        assert indicators == TestWorldBank.indicators
        assert tags == ['Agriculture', 'Rural Development', 'Environment']

    def test_get_countries(self, downloader):
        countries = get_countries('http://haha/', downloader)
        countries = list(countries)
        assert countries == [('ABW', 'Aruba'), ('AFG', 'Afghanistan'), ('AGO', 'Angola')]

    def test_generate_dataset(self, configuration, downloader):
        base_url = Configuration.read()['base_url']
        dataset, topline_indicators = generate_dataset(base_url, downloader, 'AFG', 'Afghanistan', TestWorldBank.indicators, ['AG.LND.TOTL.K2'])
        assert dataset == {'title': 'World Bank Indicators for Afghanistan', 'groups': [{'name': 'afg'}],
                           'data_update_frequency': '365', 'dataset_date': '01/01/2014-12/31/2016',
                           'tags': [{'name': 'indicators'}, {'name': 'World Bank'}],
                           'name': 'world-bank-indicators-for-afghanistan',
                           'maintainer': '196196be-6037-4488-8b71-d786adf4c081', 'owner_org': 'hdx'}
        assert topline_indicators == TestWorldBank.topline_indicators
        resources = dataset.get_resources()
        assert resources == [{'name': 'Land area (sq. km)', 'format': 'json',
                              'description': "Source: Food and Agriculture Organization, electronic files and web site.  \n   \nLand area is a country's total area... and lakes.",
                              'url': '%scountries/AFG/indicators/AG.LND.TOTL.K2?format=json&per_page=10000' % base_url}]

    def test_generate_topline_dataset(self, configuration):
        folder = gettempdir()
        dataset = generate_topline_dataset(folder, TestWorldBank.topline_indicators, ['AFG'])
        assert dataset == {'name': 'world-bank-country-topline-indicators', 'groups': [{'name': 'afg'}],
                           'tags': [{'name': 'indicators'}, {'name': 'World Bank'}], 'owner_org': 'hdx',
                           'title': 'World Bank Country Topline Indicators',
                           'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                           'dataset_date': '01/01/2016-12/31/2016', 'data_update_frequency': '365'}

