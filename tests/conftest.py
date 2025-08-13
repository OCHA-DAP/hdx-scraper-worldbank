from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.useragent import UserAgent

from tests.countries_data import CountriesData
from tests.indicators_data import IndicatorsData
from tests.other_data import OtherData
from tests.topics_data import TopicsData


@pytest.fixture(scope="session")
def configuration():
    UserAgent.set_global("test")
    Configuration._create(
        hdx_read_only=True,
        hdx_site="feature",
        project_config_yaml=join("tests", "config", "project_configuration.yaml"),
    )
    Locations.set_validlocations([{"name": "afg", "title": "Afghanistan"}])
    Country.countriesdata(False)
    tags = (
        "hxl",
        "gender",
        "economics",
        "poverty",
        "health",
        "population",
        "indicators",
    )
    Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
    tags = [{"name": tag} for tag in tags]
    Vocabulary._approved_vocabulary = {
        "tags": tags,
        "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
        "name": "approved",
    }
    return Configuration.read()


@pytest.fixture(scope="session")
def downloader():
    class Response:
        @staticmethod
        def json():
            pass

    class Download:
        topics = [
            {
                i: TopicsData.topics[0][i].replace("and", "&")
                for i in TopicsData.topics[0]
                if i not in ["tags", "sources"]
            },
            {
                i: TopicsData.topics[1][i]
                for i in TopicsData.topics[1]
                if i not in ["tags", "sources"]
            },
            {
                i: TopicsData.topics[2][i]
                for i in TopicsData.topics[2]
                if i not in ["tags", "sources"]
            },
            {
                i: TopicsData.topics[3][i]
                for i in TopicsData.topics[3]
                if i not in ["tags", "sources"]
            },
            {
                i: TopicsData.topics[4][i]
                for i in TopicsData.topics[4]
                if i not in ["tags", "sources"]
            },
        ]

        @staticmethod
        def download(url):
            response = Response()
            if url == "http://lala/v2/en/source?format=json&per_page=10000":

                def fn():
                    return [None, OtherData.sources]

                response.json = fn
            elif url == "http://lala/v2/en/topic?format=json&per_page=10000":

                def fn():
                    return [None, Download.topics]

                response.json = fn
            elif (
                url == "http://lala/v2/en/topic/17/indicator?format=json&per_page=10000"
            ):

                def fn():
                    return [None, TopicsData.gender]

                response.json = fn
            elif (
                url == "http://lala/v2/en/topic/11/indicator?format=json&per_page=10000"
            ):

                def fn():
                    return [None, TopicsData.poverty]

                response.json = fn
            elif (
                url == "http://lala/v2/en/topic/8/indicator?format=json&per_page=10000"
            ):

                def fn():
                    return [None, TopicsData.health]

                response.json = fn
            elif (
                url == "http://lala/v2/en/topic/95/indicator?format=json&per_page=10000"
            ):

                def fn():
                    return [None, TopicsData.population]

                response.json = fn
            elif (
                url == "http://lala/v2/en/topic/99/indicator?format=json&per_page=10000"
            ):

                def fn():
                    return [None, TopicsData.economics]

                response.json = fn
            elif url == "http://haha/v2/en/country?format=json&per_page=10000":

                def fn():
                    return [None, CountriesData.countries]

                response.json = fn
            elif (
                url
                == "http://papa/v2/en/country/AFG/indicator/SH.STA.MMRT;SG.LAW.CHMR;SP.ADO.TFRT;SH.MMR.RISK?source=2&format=json&per_page=10000"
            ):

                def fn():
                    return IndicatorsData.indicators

                response.json = fn
            elif (
                url
                == "http://papa/v2/en/country/AFG/indicator/SI.POV.GAPS?source=2&format=json&per_page=10000"
            ):

                def fn():
                    return IndicatorsData.indicatorsp

                response.json = fn
            elif (
                url
                == "http://papa/v2/en/country/AFG/indicator/SP.POP.TOTL?source=2&format=json&per_page=10000"
            ):

                def fn():
                    return IndicatorsData.indicatorsh

                response.json = fn
            elif (
                url
                == "http://papa/v2/en/country/AFG/indicator/SP2.POP.TOTL?source=2&format=json&per_page=10000"
            ):

                def fn():
                    return IndicatorsData.indicatorsx

                response.json = fn
            elif (
                url
                == "http://papa/v2/en/country/AFG/indicator/SH.STA.MMRT;SG.LAW.CHMR?source=2&format=json&per_page=10000"
            ):

                def fn():
                    return IndicatorsData.indicators1

                response.json = fn
            elif (
                url
                == "http://papa/v2/en/country/AFG/indicator/SP.ADO.TFRT;SH.MMR.RISK?source=2&format=json&per_page=10000"
            ):

                def fn():
                    return IndicatorsData.indicators2

                response.json = fn
            elif (
                url
                == "http://papa/v2/en/country/all/indicator/SP.POP.TOTL?source=2&mrnev=1&format=json&per_page=10000"
            ):

                def fn():
                    return IndicatorsData.indicatorst

                response.json = fn
            elif (
                url
                == "http://lala/v2/en/country/all/indicator/SP.POP.TOTL?source=2&mrnev=1&format=json&per_page=10000"
            ):

                def fn():
                    return IndicatorsData.indicatorsp

                response.json = fn
            elif (
                url
                == "http://haha/v2/en/country/all/indicator/SP.POP.TOTL?source=2&mrnev=1&format=json&per_page=10000"
            ):

                def fn():
                    return IndicatorsData.indicatorsx

                response.json = fn

            return response

    return Download()
