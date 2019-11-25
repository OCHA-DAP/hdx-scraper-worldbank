#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for worldbank.

"""
import copy
from os.path import join

import pytest
from hdx.data.dataset import Dataset
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.path import temp_dir
from slugify import slugify

from worldbank import generate_dataset_and_showcase, get_topics, get_countries, generate_topline_dataset, \
    generate_resource_view, get_unit, generate_all_datasets_showcases, generate_combined_dataset_and_showcase


class TestWorldBank:
    country = {'name': 'Afghanistan', 'iso3': 'AFG', 'iso2': 'AF'}
    madeupcountry = {'name': 'XCountry', 'iso3': 'XYZ', 'iso2': 'XY'}

    sources = [{'id': '2', 'lastupdated': '2019-09-27', 'name': 'World Development Indicators', 'code': 'WDI', 'description': '', 'url': '', 'dataavailability': 'Y', 'metadataavailability': 'Y', 'concepts': '3'},
               {'id': '57', 'lastupdated': '2019-05-01', 'name': 'WDI Database Archives', 'code': 'WDA', 'description': '', 'url': '', 'dataavailability': 'Y', 'metadataavailability': 'Y', 'concepts': '4'},
               {'id': '62', 'lastupdated': '2017-05-09', 'name': 'International Comparison Program (ICP) 2011', 'code': 'ICP', 'description': '', 'url': '', 'dataavailability': 'N', 'metadataavailability': 'Y', 'concepts': '4'}]
    gender = [{'id': 'SH.STA.MMRT', 'name': 'Maternal mortality ratio (modeled estimate, per 100, 000 live births)', 'unit': '', 'source': {'id': '2', 'value': 'World Development Indicators'}, 'sourceNote': 'Maternal mortality ratio is ...', 'sourceOrganization': 'WHO, UNICEF, UNFPA, World Bank Group, and the United Nations Population Division. Trends in Maternal Mortality:  2000 to 2017. Geneva, World Health Organization, 2019', 'topics': [{'id': '8', 'value': 'Health '}, {'id': '17', 'value': 'Gender'}, {'id': '2', 'value': 'Aid Effectiveness '}]},
              {'id': 'SG.LAW.CHMR', 'name': 'Law prohibits or invalidates child or early marriage (1=yes; 0=no)', 'unit': '', 'source': {'id': '2', 'value': 'World Development Indicators'}, 'sourceNote': 'Law prohibits or invalidates...', 'sourceOrganization': 'World Bank: Women, Business and the Law.', 'topics': [{'id': '13', 'value': 'Public Sector '}, {'id': '17', 'value': 'Gender'}]},
              {'id': 'SP.ADO.TFRT', 'name': 'Adolescent fertility rate (births per 1,000 women ages 15-19)', 'unit': '', 'source': {'id': '2', 'value': 'World Development Indicators'}, 'sourceNote': 'Adolescent fertility rate is...', 'sourceOrganization': 'United Nations Population Division,  World Population Prospects.', 'topics': [{'id': '8', 'value': 'Health '}, {'id': '17', 'value': 'Gender'}, {'id': '15', 'value': 'Social Development '}]},
              {'id': 'SH.MMR.RISK', 'name': 'Lifetime risk of maternal death (1 in: rate varies by country)', 'unit': '', 'source': {'id': '2', 'value': 'World Development Indicators'}, 'sourceNote': 'Life time risk of maternal death is...', 'sourceOrganization': 'WHO, UNICEF, UNFPA, World Bank Group, and the United Nations Population Division. Trends in Maternal Mortality:  2000 to 2017. Geneva, World Health Organization, 2019', 'topics': [{'id': '8', 'value': 'Health '}, {'id': '17', 'value': 'Gender'}]}]
    poverty = [{'id': 'SI.POV.GAPS', 'name': 'Poverty gap at $1.90 a day (2011 PPP) (%)', 'unit': '', 'source': {'id': '2', 'value': 'World Development Indicators'}, 'sourceNote': 'Poverty gap at $1.90 a day (2011 PPP)..', 'sourceOrganization': 'World Bank, Development Research Group.', 'topics': [{'id': '11','value': 'Poverty '}]}]
    health = [{'id': 'SP.POP.TOTL', 'name': 'Population, total', 'unit': '', 'source': {'id': '2', 'value': 'World Development Indicators'}, 'sourceNote': 'Total population is based on the de facto definition of population..', 'sourceOrganization': '(1) United Nations Population Division. World Population Prospects: 2019 Revision...', 'topics': [{'id': '11', 'value': 'Climate Change'}, {'id': '8', 'value': 'Health '}]}]
    population = [{'id': 'SP2.POP.TOTL', 'name': 'Population2, total', 'unit': '', 'source': {'id': '2', 'value': 'World Development Indicators'}, 'sourceNote': 'Total population is based on the de facto definition of population..', 'sourceOrganization': '(1) United Nations Population Division. World Population Prospects: 2019 Revision...', 'topics': [{'id': '11', 'value': 'Climate Change'}, {'id': '8', 'value': 'Health '}]}]
    economics = [{'id': 'XX.YYY.ZZZZ', 'name': 'Economics', 'unit': '', 'source': {'id': '5', 'value': 'Something'}, 'sourceNote': 'Something..', 'sourceOrganization': 'Someone...', 'topics': [{'id': '99', 'value': 'Economics'}]}]
    topics = [{'id': '17', 'value': 'Gender and Science', 'sourceNote': 'Gender equality is a core development objective...', 'tags': ['gender', 'science'], 'sources': {'2': gender}},
              {'id': '11', 'value': 'Poverty', 'sourceNote': 'For countries with an active poverty monitoring program...', 'tags': ['poverty'], 'sources': {'2': poverty}},
              {'id': '8', 'value': 'Health', 'sourceNote': 'Improving health is central to the Millennium Development Goals...', 'tags': ['health'], 'sources': {'2': health}},
              {'id': '99', 'value': 'Economics', 'sourceNote': 'Something...', 'tags': ['economics'], 'sources': dict()},
              {'id': '95', 'value': 'Population', 'sourceNote': 'Improving health is central to the Millennium Development Goals...', 'tags': ['population'], 'sources': {'2': population}}]
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
    dataset = {'name': 'world-bank-gender-and-science-indicators-for-afghanistan',
               'title': 'Afghanistan - Gender and Science', 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
               'owner_org': 'hdx', 'subnational': '0', 'groups': [{'name': 'afg'}], 'data_update_frequency': '365',
               'tags': [{'name': 'gender', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'economics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'indicators', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
               'notes': "Contains data from the World Bank's [data portal](http://data.worldbank.org/). There is also a [consolidated country dataset](https://test-data.humdata.org/dataset/world-bank-combined-indicators-for-afghanistan) on HDX.\n\nGender equality is a core development objective...\n\nIndicators: Adolescent fertility rate, Law prohibits or invalidates child or early marriage, Lifetime risk of maternal death, Maternal mortality ratio",
               'dataset_date': '01/01/2016-12/31/2017', 'dataset_preview': 'resource_id'}
    resource = {'name': 'Gender and Science Indicators for Afghanistan', 'description': 'HXLated csv containing Gender and Science indicators',
                'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload', 'dataset_preview_enabled': 'True'}
    indicatorsp = [{'page': 1, 'pages': 1, 'per_page': 10000, 'total': 0, 'sourceid': None, 'lastupdated': '2019-10-02'},
                   [{'indicator': {'id': 'SI.POV.GAPS', 'value': 'Poverty gap at $1.90 a day (2011 PPP) (%)'}, 'country': {'id': 'AW','value': 'Aruba'}, 'countryiso3code': 'ABW', 'date': '2019', 'value': None, 'unit': '', 'obs_status': '', 'decimal': 1}]]
    indicatorsh = [{'page': 1, 'pages': 1, 'per_page': 10000, 'total': 10, 'sourceid': None, 'lastupdated': '2019-10-02'},
                   [{'indicator': {'id': 'SP.POP.TOTL', 'value': 'Population, total'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2018', 'value': 37172386, 'unit': '', 'obs_status': '', 'decimal': 0}]]
    indicatorsx = [{'page': 1, 'pages': 2, 'per_page': 10000, 'total': 10, 'sourceid': None, 'lastupdated': '2019-10-02'},
                   [{'indicator': {'id': 'SP2.POP.TOTL', 'value': 'Population, total'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2018', 'value': 37172386, 'unit': '', 'obs_status': '', 'decimal': 0}]]
    indicators1 = [{'page': 1, 'pages': 1, 'per_page': 10000, 'total': 236, 'sourceid': None, 'lastupdated': '2019-10-02'},
                   [{'indicator': {'id': 'SH.STA.MMRT', 'value': 'Maternal mortality ratio (modeled estimate, per 100,000 live births)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2016', 'value': 673, 'unit': '', 'obs_status': '', 'decimal': 0},
                    {'indicator': {'id': 'SG.LAW.CHMR', 'value': 'Law prohibits or invalidates child or early marriage (1=yes; 0=no)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2016', 'value': 1, 'unit': '', 'obs_status': '', 'decimal': 1}]]
    indicators2 = [{'page': 1, 'pages': 1, 'per_page': 10000, 'total': 236, 'sourceid': None, 'lastupdated': '2019-10-02'},
                   [{'indicator': {'id': 'SP.ADO.TFRT', 'value': 'Adolescent fertility rate (births per 1,000 women ages 15-19)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2016', 'value': 75.325, 'unit': '', 'obs_status': '', 'decimal': 0},
                    {'indicator': {'id': 'SH.MMR.RISK', 'value': 'Lifetime risk of maternal death (1 in:  rate varies by country)'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2016', 'value': 30, 'unit': '', 'obs_status': '', 'decimal': 0}]]
    indicatorst = [{'page': 1, 'pages': 1, 'per_page': 10000, 'total': 236, 'sourceid': None, 'lastupdated': '2019-10-02'},
                   [{'indicator': {'id': 'SP.POP.TOTL', 'value': 'Population, total'}, 'country': {'id': 'AF', 'value': 'Afghanistan'}, 'countryiso3code': 'AFG', 'date': '2018', 'value': 37172386, 'unit': '', 'obs_status': '', 'decimal': 0},
                    {'indicator': {'id': 'SP.POP.TOTL', 'value': 'Population, total'}, 'country': {'id': 'XX', 'value': 'XCountry'}, 'countryiso3code': 'XYZ', 'date': '2018', 'value': 1, 'unit': '', 'obs_status': '', 'decimal': 0},
                    {'indicator': {'id': 'SP.POP.TOTL', 'value': 'Population, total'}, 'country': {'id': 'YY', 'value': 'YCountry'}, 'countryiso3code': 'YYZ', 'date': '2018', 'value': 1, 'unit': '', 'obs_status': '', 'decimal': 0}]]

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, hdx_site='test', user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}])
        Country.countriesdata(False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'hxl'}, {'name': 'gender'}, {'name': 'economics'}, {'name': 'poverty'}, {'name': 'health'}, {'name': 'population'}, {'name': 'indicators'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}
        return Configuration.read()

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            topics = [{i: TestWorldBank.topics[0][i].replace('and', '&') for i in TestWorldBank.topics[0] if i not in ['tags', 'sources']},
                      {i: TestWorldBank.topics[1][i] for i in TestWorldBank.topics[1] if i not in ['tags', 'sources']},
                      {i: TestWorldBank.topics[2][i] for i in TestWorldBank.topics[2] if i not in ['tags', 'sources']},
                      {i: TestWorldBank.topics[3][i] for i in TestWorldBank.topics[3] if i not in ['tags', 'sources']},
                      {i: TestWorldBank.topics[4][i] for i in TestWorldBank.topics[4] if i not in ['tags', 'sources']}]

            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://lala/v2/en/source?format=json&per_page=10000':
                    def fn():
                        return [None, TestWorldBank.sources]
                    response.json = fn
                elif url == 'http://lala/v2/en/topic?format=json&per_page=10000':
                    def fn():
                        return [None, Download.topics]
                    response.json = fn
                elif url == 'http://lala/v2/en/topic/17/indicator?format=json&per_page=10000':
                    def fn():
                        return [None, TestWorldBank.gender]
                    response.json = fn
                elif url == 'http://lala/v2/en/topic/11/indicator?format=json&per_page=10000':
                    def fn():
                        return [None, TestWorldBank.poverty]
                    response.json = fn
                elif url == 'http://lala/v2/en/topic/8/indicator?format=json&per_page=10000':
                    def fn():
                        return [None, TestWorldBank.health]
                    response.json = fn
                elif url == 'http://lala/v2/en/topic/95/indicator?format=json&per_page=10000':
                    def fn():
                        return [None, TestWorldBank.population]
                    response.json = fn
                elif url == 'http://lala/v2/en/topic/99/indicator?format=json&per_page=10000':
                    def fn():
                        return [None, TestWorldBank.economics]
                    response.json = fn
                elif url == 'http://haha/v2/en/country?format=json&per_page=10000':
                    def fn():
                        return [None, TestWorldBank.countries]
                    response.json = fn
                elif url == 'http://papa/v2/en/country/AFG/indicator/SH.STA.MMRT;SG.LAW.CHMR;SP.ADO.TFRT;SH.MMR.RISK?source=2&format=json&per_page=10000':
                    def fn():
                        return TestWorldBank.indicators
                    response.json = fn
                elif url == 'http://papa/v2/en/country/AFG/indicator/SI.POV.GAPS?source=2&format=json&per_page=10000':
                    def fn():
                        return TestWorldBank.indicatorsp
                    response.json = fn
                elif url == 'http://papa/v2/en/country/AFG/indicator/SP.POP.TOTL?source=2&format=json&per_page=10000':
                    def fn():
                        return TestWorldBank.indicatorsh
                    response.json = fn
                elif url == 'http://papa/v2/en/country/AFG/indicator/SP2.POP.TOTL?source=2&format=json&per_page=10000':
                    def fn():
                        return TestWorldBank.indicatorsx
                    response.json = fn
                elif url == 'http://papa/v2/en/country/AFG/indicator/SH.STA.MMRT;SG.LAW.CHMR?source=2&format=json&per_page=10000':
                    def fn():
                        return TestWorldBank.indicators1
                    response.json = fn
                elif url == 'http://papa/v2/en/country/AFG/indicator/SP.ADO.TFRT;SH.MMR.RISK?source=2&format=json&per_page=10000':
                    def fn():
                        return TestWorldBank.indicators2
                    response.json = fn
                elif url == 'http://papa/v2/en/country/all/indicator/SP.POP.TOTL?source=2&mrnev=1&format=json&per_page=10000':
                    def fn():
                        return TestWorldBank.indicatorst
                    response.json = fn
                elif url == 'http://lala/v2/en/country/all/indicator/SP.POP.TOTL?source=2&mrnev=1&format=json&per_page=10000':
                    def fn():
                        return TestWorldBank.indicatorsp
                    response.json = fn
                elif url == 'http://haha/v2/en/country/all/indicator/SP.POP.TOTL?source=2&mrnev=1&format=json&per_page=10000':
                    def fn():
                        return TestWorldBank.indicatorsx
                    response.json = fn

                return response
        return Download()

    def test_get_topics(self, downloader):
        topics = get_topics('http://lala/', downloader)
        assert topics == TestWorldBank.topics

    def test_get_countries(self, downloader):
        countries = get_countries('http://haha/', downloader)
        assert list(countries) == [TestWorldBank.country]

    def test_get_unit(self):
        assert get_unit('Rural population (% of total population)') == '% of total population'
        assert get_unit('Agricultural machinery, tractors') == 'tractors'
        assert get_unit('School enrollment, primary (gross), gender parity index (GPI)') == 'gross GPI'
        assert get_unit('Educational attainment, at least completed primary, population 25+ years, female (%) (cumulative)') == '% cumulative'
        assert get_unit('Insurance and financial services (% of service exports, BoP)') == '% of service exports, BoP'
        assert get_unit('Distance to frontier score (0=lowest performance to 100=frontier)') == '0=lowest performance to 100=frontier'
        assert get_unit('Problems in accessing health care (not wanting to go alone) (% of women): Q1 (lowest)') == 'not wanting to go alone % of women lowest'
        assert get_unit('Female population 80+') == 'people'
        assert get_unit('Annualized Mean Income Growth Bottom 40 Percent (2004-2014)') == 'Annualized Mean Income Growth Bottom 40 Percent (2004-2014)'
        assert get_unit('Barro-Lee: Percentage of female population age 15-19 with no education') == '%'
        assert get_unit('MICS: Net attendance rate. Secondary. Male') == 'MICS: Net attendance rate. Secondary. Male'
        assert get_unit('Mobile account, income, richest 60% (% ages 15+)') == '% ages 15+'
        assert get_unit('Adolescent fertility rate (births per 1,000 women ages 15-19)') == 'births per 1,000 women ages 15-19'
        assert get_unit('Youth: Neither in School Nor Working  (15-24)') == 'Youth: Neither in School Nor Working  (15-24)'
        assert get_unit('Average per capita transfer held by 1st quintile (poorest) - Active Labor Market') == 'Average per capita transfer held by 1st quintile (poorest) - Active Labor Market'
        assert get_unit('Poverty Headcount ($1.90 a day)') == 'Poverty Headcount ($1.90 a day)'
        assert get_unit('Poverty Severity ($4 a day)-Urban') == 'Poverty Severity ($4 a day)-Urban'
        assert get_unit('Coverage: Mathematics Proficiency Level 2, Private schools') == 'Coverage Rate'
        assert get_unit('Mean Log Deviation, GE(0)') == 'GE(0)'
        assert get_unit('Mean Log Deviation, GE(0), Rural') == 'GE(0), Rural'
        assert get_unit('Number of listed companies per 1,000,000 people ') == 'listed companies per 1,000,000 people'
        assert get_unit('Number of deaths ages 5-14 years') == 'deaths ages 5-14 years'

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        with temp_dir('worldbank') as folder:
            site_url = configuration.get_hdx_site_url()
            topic = TestWorldBank.topics[0]
            dataset, showcase, qc_indicators, earliest_year, latest_year, rows = \
                generate_dataset_and_showcase(site_url, configuration, downloader, folder, TestWorldBank.country, topic)
            assert dataset == TestWorldBank.dataset
            resource = dataset.get_resource()
            assert resource == TestWorldBank.resource
            filename = '%s_%s.csv' % (slugify(topic['value']), TestWorldBank.country['iso3'])
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
                                         {'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'indicators', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}
            assert qc_indicators == TestWorldBank.qc_indicators
            assert earliest_year == 2016
            assert latest_year == 2017
            assert len(rows) == 9

            dataset, _, _, _, _, topicname = generate_dataset_and_showcase(site_url, configuration, downloader, folder,
                                                                           TestWorldBank.madeupcountry, topic)
            assert topicname == 'Gender and Science'
            configuration['character_limit'] = 25
            configuration['indicator_subtract'] = 2
            dataset, _, _, _, _, _ = generate_dataset_and_showcase(site_url, configuration, downloader, folder,
                                                                   TestWorldBank.country, topic)
            filename = '%s_%s.csv' % (slugify(topic['value']), TestWorldBank.country['iso3'])
            expected_file = join('tests', 'fixtures', 'split_%s' % filename)
            actual_file = join(folder, filename)
            assert_files_same(expected_file, actual_file)

    def test_generate_combined_dataset_and_showcase(self):
        dataset, showcase, bites_disabled = generate_combined_dataset_and_showcase(None, None, TestWorldBank.madeupcountry, None, None, None, None, None, None)
        assert dataset is None
        assert showcase is None
        assert bites_disabled is None

    def test_generate_all_datasets_showcases(self, configuration, downloader):
        def create_dataset_showcase(dataset, showcase, qc_indicators):
            pass
        with temp_dir('worldbank') as folder:
            dataset, showcase, bites_disabled = generate_all_datasets_showcases(configuration, downloader, folder, TestWorldBank.country,
                                                                                TestWorldBank.topics[:4], create_dataset_showcase)
            assert dataset == {'name': 'world-bank-combined-indicators-for-afghanistan', 'title': 'Afghanistan - Economic, Social, Environmental, Health, Education, Development and Energy',
                               'maintainer': '196196be-6037-4488-8b71-d786adf4c081', 'owner_org': 'hdx', 'subnational': '0', 'groups': [{'name': 'afg'}], 'data_update_frequency': '365',
                               'tags': [{'name': 'economics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'gender', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'health', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'indicators', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                               'notes': "Contains data from the World Bank's [data portal](http://data.worldbank.org/) covering the following topics which also exist as individual datasets on HDX: [Gender and Science](https://test-data.humdata.org/dataset/world-bank-gender-and-science-indicators-for-afghanistan), [Health](https://test-data.humdata.org/dataset/world-bank-health-indicators-for-afghanistan).", 'dataset_date': '01/01/2016-12/31/2018'}
            resources = dataset.get_resources()
            assert resources == [{'name': 'Combined Indicators for Afghanistan', 'description': 'HXLated csv containing Economic, Social, Environmental, Health, Education, Development and Energy indicators',
                                  'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
                                 {'name': 'QuickCharts Combined Indicators for Afghanistan', 'description': 'QuickCharts resource',
                                  'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'}]
            filename = 'indicators_%s.csv' % TestWorldBank.country['iso3']
            expected_file = join('tests', 'fixtures', filename)
            actual_file = join(folder, filename)
            assert_files_same(expected_file, actual_file)
            assert showcase == {'name': 'world-bank-combined-indicators-for-afghanistan-showcase', 'title': 'Indicators for Afghanistan',
                                'notes': 'Economic, Social, Environmental, Health, Education, Development and Energy indicators for Afghanistan',
                                'url': 'https://data.worldbank.org/?locations=AF', 'image_url': 'https://www.worldbank.org/content/dam/wbr/logo/logo-wb-header-en.svg',
                                'tags': [{'name': 'economics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'gender', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'health', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'indicators', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}
            assert bites_disabled == [False, True, True]
            dataset, showcase, bites_disabled = generate_all_datasets_showcases(configuration, downloader, folder, TestWorldBank.country,
                                                                                [TestWorldBank.topics[1]], create_dataset_showcase)
            assert dataset is None
            assert showcase is None
            assert bites_disabled is None

            with pytest.raises(ValueError):
                _ = generate_all_datasets_showcases(configuration, downloader, folder, TestWorldBank.country,
                                                    TestWorldBank.topics, create_dataset_showcase)

    def test_generate_topline_dataset(self, configuration, downloader):
        with temp_dir('worldbank') as folder:
            countries = [TestWorldBank.country, {'iso3': 'YYZ'}]
            topline_indicators = configuration['topline_indicators']
            dataset = generate_topline_dataset(configuration['base_url'], downloader, folder, countries, topline_indicators)
            assert dataset == {'name': 'world-bank-country-topline-indicators', 'title': 'Topline Indicators',
                               'maintainer': '196196be-6037-4488-8b71-d786adf4c081', 'owner_org': 'hdx', 'subnational': '0',
                               'groups': [{'name': 'afg'}], 'data_update_frequency': '365', 'dataset_date': '01/01/2018-12/31/2018',
                               'tags': [{'name': 'indicators', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}
            filename = 'worldbank_topline.csv'
            expected_file = join('tests', 'fixtures', filename)
            actual_file = join(folder, filename)
            assert_files_same(expected_file, actual_file)

            with pytest.raises(ValueError):
                _ = generate_topline_dataset('http://lala/', downloader, folder, countries, topline_indicators)
            with pytest.raises(ValueError):
                _ = generate_topline_dataset('http://haha/', downloader, folder, countries, topline_indicators)

    def test_generate_resource_view(self, configuration):
        dataset = Dataset(TestWorldBank.dataset)
        resource = copy.deepcopy(TestWorldBank.resource)
        resource['id'] = '123'
        resource['url'] = 'https://test-data.humdata.org/dataset/b135da4b-5623-431a-93ff-936df331c762/resource/1ef32898-529e-4976-b7bb-6f04438e9bea/download/gender_afg.csv'
        dataset.add_update_resource(resource)
        qc_indicators = copy.deepcopy(TestWorldBank.qc_indicators)
        result = generate_resource_view(dataset, qc_indicators)
        assert result == {'resource_id': '123', 'description': '', 'title': 'Quick Charts', 'view_type': 'hdx_hxl_preview',
                          'hxl_preview_config': '{"configVersion":5,"bites":[{"tempShowSaveCancelButtons":false,"ingredient":{"aggregateColumn":null,"valueColumn":"#indicator+value+num","aggregateFunction":"sum","dateColumn":"#date+year","comparisonValueColumn":null,"comparisonOperator":null,"filters":{"filterWith":[{"#indicator+code":"SH.STA.MMRT"}]},"description":""},"type":"timeseries","errorMsg":null,"computedProperties":{"explainedFiltersMap":{"remove empty valued rows":{"filterWith":[{"#indicator+value+num":"is not empty"}],"filterWithout":[]}},"pieChart":false,"title":"Sum of Value by Year","dataTitle":"modeled estimate, per 100,000 live births"},"uiProperties":{"swapAxis":true,"showGrid":true,"color":"#0077ce","sortingByValue1":"DESC","sortingByCategory1":null,"showPoints":true,"internalColorPattern":["#1ebfb3","#0077ce","#f2645a","#9C27B0"],"title":"Maternal mortality ratio (modeled estimate, per 100,000 live births)"},"dataProperties":{},"displayCategory":"Timeseries","hashCode":498306664},{"tempShowSaveCancelButtons":false,"ingredient":{"aggregateColumn":null,"valueColumn":"#indicator+value+num","aggregateFunction":"sum","dateColumn":"#date+year","comparisonValueColumn":null,"comparisonOperator":null,"filters":{"filterWith":[{"#indicator+code":"SP.ADO.TFRT"}]},"description":""},"type":"timeseries","errorMsg":null,"computedProperties":{"explainedFiltersMap":{"remove empty valued rows":{"filterWith":[{"#indicator+value+num":"is not empty"}],"filterWithout":[]}},"pieChart":false,"title":"Sum of Value by Year","dataTitle":"births per 1,000 women ages 15-19"},"uiProperties":{"swapAxis":true,"showGrid":true,"color":"#0077ce","sortingByValue1":"DESC","sortingByCategory1":null,"showPoints":true,"internalColorPattern":["#1ebfb3","#0077ce","#f2645a","#9C27B0"],"title":"Adolescent fertility rate (births per 1,000 women ages 15-19)"},"dataProperties":{},"displayCategory":"Timeseries","hashCode":498306664},{"tempShowSaveCancelButtons":false,"ingredient":{"aggregateColumn":null,"valueColumn":"#indicator+value+num","aggregateFunction":"sum","dateColumn":"#date+year","comparisonValueColumn":null,"comparisonOperator":null,"filters":{"filterWith":[{"#indicator+code":"SH.MMR.RISK"}]},"description":""},"type":"timeseries","errorMsg":null,"computedProperties":{"explainedFiltersMap":{"remove empty valued rows":{"filterWith":[{"#indicator+value+num":"is not empty"}],"filterWithout":[]}},"pieChart":false,"title":"Sum of Value by Year","dataTitle":"1 in:  rate varies by country"},"uiProperties":{"swapAxis":true,"showGrid":true,"color":"#0077ce","sortingByValue1":"DESC","sortingByCategory1":null,"showPoints":true,"internalColorPattern":["#1ebfb3","#0077ce","#f2645a","#9C27B0"],"title":"Lifetime risk of maternal death (1 in:  rate varies by country)"},"dataProperties":{},"displayCategory":"Timeseries","hashCode":498306664}],"recipeUrl":"https://raw.githubusercontent.com/mcarans/hxl-recipes/dev/recipes/historic-values-filtered/recipe.json"}'}
        qc_indicators[2] = None
        result = generate_resource_view(dataset, qc_indicators)
        assert result == {'resource_id': '123', 'description': '', 'title': 'Quick Charts', 'view_type': 'hdx_hxl_preview',
                          'hxl_preview_config': '{"configVersion": 5, "bites": [{"tempShowSaveCancelButtons": false, "ingredient": {"aggregateColumn": null, "valueColumn": "#indicator+value+num", "aggregateFunction": "sum", "dateColumn": "#date+year", "comparisonValueColumn": null, "comparisonOperator": null, "filters": {"filterWith": [{"#indicator+code": "SH.STA.MMRT"}]}, "description": ""}, "type": "timeseries", "errorMsg": null, "computedProperties": {"explainedFiltersMap": {"remove empty valued rows": {"filterWith": [{"#indicator+value+num": "is not empty"}], "filterWithout": []}}, "pieChart": false, "title": "Sum of Value by Year", "dataTitle": "modeled estimate, per 100,000 live births"}, "uiProperties": {"swapAxis": true, "showGrid": true, "color": "#0077ce", "sortingByValue1": "DESC", "sortingByCategory1": null, "showPoints": true, "internalColorPattern": ["#1ebfb3", "#0077ce", "#f2645a", "#9C27B0"], "title": "Maternal mortality ratio (modeled estimate, per 100,000 live births)"}, "dataProperties": {}, "displayCategory": "Timeseries", "hashCode": 498306664}, {"tempShowSaveCancelButtons": false, "ingredient": {"aggregateColumn": null, "valueColumn": "#indicator+value+num", "aggregateFunction": "sum", "dateColumn": "#date+year", "comparisonValueColumn": null, "comparisonOperator": null, "filters": {"filterWith": [{"#indicator+code": "SP.ADO.TFRT"}]}, "description": ""}, "type": "timeseries", "errorMsg": null, "computedProperties": {"explainedFiltersMap": {"remove empty valued rows": {"filterWith": [{"#indicator+value+num": "is not empty"}], "filterWithout": []}}, "pieChart": false, "title": "Sum of Value by Year", "dataTitle": "births per 1,000 women ages 15-19"}, "uiProperties": {"swapAxis": true, "showGrid": true, "color": "#0077ce", "sortingByValue1": "DESC", "sortingByCategory1": null, "showPoints": true, "internalColorPattern": ["#1ebfb3", "#0077ce", "#f2645a", "#9C27B0"], "title": "Adolescent fertility rate (births per 1,000 women ages 15-19)"}, "dataProperties": {}, "displayCategory": "Timeseries", "hashCode": 498306664}], "recipeUrl": "https://raw.githubusercontent.com/mcarans/hxl-recipes/dev/recipes/historic-values-filtered/recipe.json"}'}
        qc_indicators[1] = None
        result = generate_resource_view(dataset, qc_indicators)
        assert result == {'resource_id': '123', 'description': '', 'title': 'Quick Charts', 'view_type': 'hdx_hxl_preview',
                          'hxl_preview_config': '{"configVersion": 5, "bites": [{"tempShowSaveCancelButtons": false, "ingredient": {"aggregateColumn": null, "valueColumn": "#indicator+value+num", "aggregateFunction": "sum", "dateColumn": "#date+year", "comparisonValueColumn": null, "comparisonOperator": null, "filters": {"filterWith": [{"#indicator+code": "SH.STA.MMRT"}]}, "description": ""}, "type": "timeseries", "errorMsg": null, "computedProperties": {"explainedFiltersMap": {"remove empty valued rows": {"filterWith": [{"#indicator+value+num": "is not empty"}], "filterWithout": []}}, "pieChart": false, "title": "Sum of Value by Year", "dataTitle": "modeled estimate, per 100,000 live births"}, "uiProperties": {"swapAxis": true, "showGrid": true, "color": "#0077ce", "sortingByValue1": "DESC", "sortingByCategory1": null, "showPoints": true, "internalColorPattern": ["#1ebfb3", "#0077ce", "#f2645a", "#9C27B0"], "title": "Maternal mortality ratio (modeled estimate, per 100,000 live births)"}, "dataProperties": {}, "displayCategory": "Timeseries", "hashCode": 498306664}], "recipeUrl": "https://raw.githubusercontent.com/mcarans/hxl-recipes/dev/recipes/historic-values-filtered/recipe.json"}'}
        qc_indicators[0] = None
        result = generate_resource_view(dataset, qc_indicators)
        assert result is None

