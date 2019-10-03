#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for worldbank.

"""
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.path import temp_dir
from slugify import slugify

from worldbank import generate_dataset_and_showcase, get_topics, get_countries, generate_topline_dataset


class TestWorldBank:
    sources = [{'id': '2', 'lastupdated': '2019-09-27', 'name': 'World Development Indicators', 'code': 'WDI', 'description': '', 'url': '', 'dataavailability': 'Y', 'metadataavailability': 'Y', 'concepts': '3'},
               {'id': '57', 'lastupdated': '2019-05-01', 'name': 'WDI Database Archives', 'code': 'WDA', 'description': '', 'url': '', 'dataavailability': 'Y', 'metadataavailability': 'Y', 'concepts': '4'},
               {'id': '62', 'lastupdated': '2017-05-09', 'name': 'International Comparison Program (ICP) 2011', 'code': 'ICP', 'description': '', 'url': '', 'dataavailability': 'N', 'metadataavailability': 'Y', 'concepts': '4'}]
    topics = [{'id': '17', 'value': 'Gender & Science', 'sourceNote': 'Gender equality is a core development objective...'}]
    gender = [{'id': 'SH.STA.MMRT', 'name': 'Maternal mortality ratio (modeled estimate, per 100, 000 live births)', 'unit': '', 'source': {'id': '2', 'value': 'World Development Indicators'}, 'sourceNote': 'Maternal mortality ratio is ...', 'sourceOrganization': 'WHO, UNICEF, UNFPA, World Bank Group, and the United Nations Population Division. Trends in Maternal Mortality:  2000 to 2017. Geneva, World Health Organization, 2019', 'topics': [{'id': '8', 'value': 'Health '}, {'id': '17', 'value': 'Gender'}, {'id': '2', 'value': 'Aid Effectiveness '}]},
              {'id': 'SG.LAW.CHMR', 'name': 'Law prohibits or invalidates child or early marriage (1=yes; 0=no)', 'unit': '', 'source': {'id': '2', 'value': 'World Development Indicators'}, 'sourceNote': 'Law prohibits or invalidates...', 'sourceOrganization': 'World Bank: Women, Business and the Law.', 'topics': [{'id': '13', 'value': 'Public Sector '}, {'id': '17', 'value': 'Gender'}]},
              {'id': 'SP.ADO.TFRT', 'name': 'Adolescent fertility rate (births per 1,000 women ages 15-19)', 'unit': '', 'source': {'id': '2', 'value': 'World Development Indicators'}, 'sourceNote': 'Adolescent fertility rate is...', 'sourceOrganization': 'United Nations Population Division,  World Population Prospects.', 'topics': [{'id': '8', 'value': 'Health '}, {'id': '17', 'value': 'Gender'}, {'id': '15', 'value': 'Social Development '}]},
              {'id': 'SH.MMR.RISK', 'name': 'Lifetime risk of maternal death (1 in: rate varies by country)', 'unit': '', 'source': {'id': '2', 'value': 'World Development Indicators'}, 'sourceNote': 'Life time risk of maternal death is...', 'sourceOrganization': 'WHO, UNICEF, UNFPA, World Bank Group, and the United Nations Population Division. Trends in Maternal Mortality:  2000 to 2017. Geneva, World Health Organization, 2019', 'topics': [{'id': '8', 'value': 'Health '}, {'id': '17', 'value': 'Gender'}]}]
    countries = {'id': 'AFG', 'iso2Code': 'AF', 'name': 'Afghanistan', 'region': {'id': 'SAS', 'iso2code': '8S', 'value': 'South Asia'}, 'adminregion': {'id': 'SAS', 'iso2code': '8S', 'value': 'South Asia'}, 'incomeLevel': {'id': 'LIC', 'iso2code': 'XM', 'value': 'Low income'}, 'lendingType': {'id': 'IDX', 'iso2code': 'XI', 'value': 'IDA'}, 'capitalCity': 'Kabul', 'longitude': '69.1761', 'latitude': '34.5228'}, \
                {'id': 'AFR', 'iso2Code': 'A9', 'name': 'Africa', 'region': {'id': 'NA', 'iso2code': 'NA', 'value': 'Aggregates'}, 'adminregion': {'id': '', 'iso2code': '', 'value': ''}, 'incomeLevel': {'id': 'NA', 'iso2code': 'NA', 'value': 'Aggregates'}, 'lendingType': {'id': '', 'iso2code': '', 'value': 'Aggregates'}, 'capitalCity': '', 'longitude': '', 'latitude': ''}
    indicators = [{'page': 1, 'pages': 1, 'per_page': 10000, 'total': 236, 'sourceid': None, 'lastupdated': '2019-10-02'},
                  [{'indicator': {'id': 'SH.STA.MMRT', 'value': 'Maternal mortality ratio (modeled estimate, per 100,000 live births)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2018', 'value': None, 'unit': '', 'obs_status': '', 'decimal': 0},
                   {'indicator': {'id': 'SH.STA.MMRT', 'value': 'Maternal mortality ratio (modeled estimate, per 100,000 live births)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2017', 'value': 638, 'unit': '', 'obs_status': '', 'decimal': 0},
                   {'indicator': {'id': 'SH.STA.MMRT', 'value': 'Maternal mortality ratio (modeled estimate, per 100,000 live births)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2016', 'value': 673, 'unit': '', 'obs_status': '', 'decimal': 0},
                   {'indicator': {'id': 'SG.LAW.CHMR', 'value': 'Law prohibits or invalidates child or early marriage (1=yes; 0=no)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2017', 'value': 1, 'unit': '', 'obs_status': '', 'decimal': 1},
                   {'indicator': {'id': 'SG.LAW.CHMR', 'value': 'Law prohibits or invalidates child or early marriage (1=yes; 0=no)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2016', 'value': 1, 'unit': '', 'obs_status': '', 'decimal': 1},
                   {'indicator': {'id': 'SP.ADO.TFRT', 'value': 'Adolescent fertility rate (births per 1,000 women ages 15-19)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2017', 'value': 68.957, 'unit': '', 'obs_status': '', 'decimal': 0},
                   {'indicator': {'id': 'SP.ADO.TFRT', 'value': 'Adolescent fertility rate (births per 1,000 women ages 15-19)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2016', 'value': 75.325, 'unit': '', 'obs_status': '', 'decimal': 0},
                   {'indicator': {'id': 'SH.MMR.RISK', 'value': 'Lifetime risk of maternal death (1 in:  rate varies by country)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2017', 'value': 33, 'unit': '', 'obs_status': '', 'decimal': 0},
                   {'indicator': {'id': 'SH.MMR.RISK', 'value': 'Lifetime risk of maternal death (1 in:  rate varies by country)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2016', 'value': 30, 'unit': '', 'obs_status': '', 'decimal': 0}]]
    qc_indicators = [{'code': 'SH.STA.MMRT', 'name': 'Maternal mortality ratio (modeled estimate, per 100,000 live births)'},
                     {'code': 'SP.ADO.TFRT', 'name': 'Adolescent fertility rate (births per 1,000 women ages 15-19)'},
                     {'code': 'SH.MMR.RISK', 'name': 'Lifetime risk of maternal death (1 in:  rate varies by country)'}]
    topline_indicator = {'countryiso': 'AFG', 'indicator': 'Adolescent fertility rate', 'source': 'World Bank',
                         'url': 'http://papa/v2/en/country/AFG/indicator/SH.STA.MMRT;SG.LAW.CHMR;SP.ADO.TFRT;SH.MMR.RISK?source=2&format=json&per_page=10000',
                         'year': 2017, 'unit': 'births per 1,000 women ages 15-19', 'value': 68.957}

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}])
        Country.countriesdata(False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'hxl'}, {'name': 'gender'}, {'name': 'economics'}, {'name': 'indicators'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://lala/v2/en/source?format=json&per_page=10000':
                    def fn():
                        return [None, TestWorldBank.sources]
                    response.json = fn
                elif url == 'http://lala/v2/en/topic?format=json&per_page=10000':
                    def fn():
                        return [None, TestWorldBank.topics]
                    response.json = fn
                elif url == 'http://lala/v2/en/topic/17/indicator?format=json&per_page=10000':
                    def fn():
                        return [None, TestWorldBank.gender]
                    response.json = fn
                elif url == 'http://haha/v2/en/country?format=json&per_page=10000':
                    def fn():
                        return [None, TestWorldBank.countries]
                    response.json = fn
                elif url == 'http://papa/v2/en/country/AFG/indicator/SH.STA.MMRT;SG.LAW.CHMR;SP.ADO.TFRT;SH.MMR.RISK?source=2&format=json&per_page=10000':
                    def fn():
                        return TestWorldBank.indicators
                    response.json = fn
                return response
        return Download()

    def test_get_topics(self, downloader):
        topics = get_topics('http://lala/', downloader)
        assert topics == [{'id': '17', 'value': 'Gender & Science', 'sourceNote': 'Gender equality is a core development objective...', 'tags': ['gender', 'science'],
                           'sources': {'2': TestWorldBank.gender}}]

    def test_get_countries(self, downloader):
        countries = get_countries('http://haha/', downloader)
        countries = list(countries)
        assert countries == [('AFG', 'AF', 'Afghanistan')]

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        with temp_dir('worldbank') as folder:
            base_url = Configuration.read()['base_url']
            indicator_limit = Configuration.read()['indicator_limit']
            character_limit = Configuration.read()['character_limit']
            tag_mappings = Configuration.read()['tag_mappings']
            topline_indicator_names = Configuration.read()['topline_indicators']
            topic = {'id': '17', 'value': 'Gender & Science', 'sourceNote': 'Gender equality is a core development objective...',
                     'tags': ['gender', 'science'], 'sources': {'2': TestWorldBank.gender}}
            topline_indicators_dict = dict()
            dataset, showcase, qc_indicators = generate_dataset_and_showcase(base_url, downloader, folder, 'AFG', 'AF', 'Afghanistan',
                                                                             topic, indicator_limit, character_limit, tag_mappings,
                                                                             topline_indicator_names, topline_indicators_dict)

            assert dataset == {'name': 'world-bank-gender-and-science-indicators-for-afghanistan',
                               'title': 'Afghanistan - Gender and Science', 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                               'owner_org': 'hdx', 'subnational': '0', 'groups': [{'name': 'afg'}], 'data_update_frequency': '365',
                               'tags': [{'name': 'gender', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                        {'name': 'economics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                        {'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                               'notes': "Gender equality is a core development objective...\n\nContains data from the World Bank's [data portal](http://data.worldbank.org/).\n\nIndicators: Adolescent fertility rate, Law prohibits or invalidates child or early marriage, Lifetime risk of maternal death, Maternal mortality ratio",
                               'dataset_date': '01/01/2016-12/31/2017', 'dataset_preview': 'resource_id'}
            resource = dataset.get_resource()
            assert resource == {'name': 'Gender and Science', 'description': 'HXLated csv containing all the Gender and Science indicators',
                                'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload', 'dataset_preview_enabled': 'True'}
            filename = '%s_AFG.csv' % (slugify(resource['name']))
            expected_file = join('tests', 'fixtures', filename)
            actual_file = join(folder, filename)
            assert_files_same(expected_file, actual_file)

            assert showcase == {'name': 'world-bank-gender-and-science-indicators-for-afghanistan-showcase',
                                'title': 'Gender and Science indicators for Afghanistan',
                                'notes': 'Gender and Science indicators for Afghanistan',
                                'url': 'https://data.worldbank.org/topic/gender-and-science?locations=AF',
                                'image_url': 'https://www.worldbank.org/content/dam/wbr/logo/logo-wb-header-en.svg',
                                'tags': [{'name': 'gender', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'economics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}
            assert qc_indicators == TestWorldBank.qc_indicators
            assert topline_indicators_dict == {'SP.ADO.TFRT': TestWorldBank.topline_indicator}

    def test_generate_topline_dataset(self, configuration):
        with temp_dir('worldbank') as folder:
            dataset = generate_topline_dataset(folder, [TestWorldBank.topline_indicator], ['AFG'])
            assert dataset == {'name': 'world-bank-country-topline-indicators', 'title': 'Topline Indicators',
                               'maintainer': '196196be-6037-4488-8b71-d786adf4c081', 'owner_org': 'hdx', 'subnational': '0',
                               'groups': [{'name': 'afg'}], 'data_update_frequency': '365', 'dataset_date': '01/01/2017-12/31/2017',
                               'tags': [{'name': 'indicators', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}

