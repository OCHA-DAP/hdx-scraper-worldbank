#!/usr/bin/python
"""
Unit tests for worldbank.

"""

from os.path import join

import pytest
from hdx.utilities.compare import assert_files_same
from hdx.utilities.path import temp_dir
from slugify import slugify

from tests.countries_data import CountriesData
from tests.other_data import OtherData
from tests.topics_data import TopicsData

from hdx.scraper.worldbank.pipeline import (
    generate_all_datasets_showcases,
    generate_combined_dataset_and_showcase,
    generate_dataset_and_showcase,
    generate_topline_dataset,
    get_countries,
    get_topics,
    get_unit,
)


class TestWorldBank:
    dataset = {
        "name": "world-bank-gender-and-science-indicators-for-afghanistan",
        "title": "Afghanistan - Gender and Science",
        "maintainer": "085d3bd8-9035-4b0e-9d2d-80e849dd7b07",
        "owner_org": "905a9a49-5325-4a31-a9d7-147a60a8387c",
        "subnational": "0",
        "groups": [{"name": "afg"}],
        "data_update_frequency": "30",
        "tags": [
            {
                "name": "economics",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {"name": "gender", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"},
            {"name": "hxl", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"},
            {
                "name": "indicators",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
        ],
        "notes": "Contains data from the World Bank's [data portal](http://data.worldbank.org/). There is also a [consolidated country dataset](https://feature.data-humdata-org.ahconu.org/dataset/world-bank-combined-indicators-for-afghanistan) on HDX.\n\nGender equality is a core development objective...",
        "dataset_date": "[2016-01-01T00:00:00 TO 2017-12-31T23:59:59]",
    }
    resources = [
        {
            "name": "Gender and Science Indicators for Afghanistan",
            "description": "HXLated csv containing Gender and Science indicators\n\nIndicators: Adolescent fertility rate, Law prohibits or invalidates child or early marriage, Lifetime risk of maternal death, Maternal mortality ratio",
            "format": "csv",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
        {
            "name": "QuickCharts-Gender and Science Indicators for Afghanistan",
            "description": "Cut down data for QuickCharts",
            "format": "csv",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
    ]

    def test_get_topics(self, downloader):
        topics = get_topics("http://lala/", downloader)
        assert topics == TopicsData.topics

    def test_get_countries(self, downloader):
        countries = get_countries("http://haha/", downloader)
        assert list(countries) == [CountriesData.country]

    def test_get_unit(self):
        assert (
            get_unit("Rural population (% of total population)")
            == "% of total population"
        )
        assert get_unit("Agricultural machinery, tractors") == "tractors"
        assert (
            get_unit("School enrollment, primary (gross), gender parity index (GPI)")
            == "gross GPI"
        )
        assert (
            get_unit(
                "Educational attainment, at least completed primary, population 25+ years, female (%) (cumulative)"
            )
            == "% cumulative"
        )
        assert (
            get_unit("Insurance and financial services (% of service exports, BoP)")
            == "% of service exports, BoP"
        )
        assert (
            get_unit(
                "Distance to frontier score (0=lowest performance to 100=frontier)"
            )
            == "0=lowest performance to 100=frontier"
        )
        assert (
            get_unit(
                "Problems in accessing health care (not wanting to go alone) (% of women): Q1 (lowest)"
            )
            == "not wanting to go alone % of women lowest"
        )
        assert get_unit("Female population 80+") == "people"
        assert (
            get_unit("Annualized Mean Income Growth Bottom 40 Percent (2004-2014)")
            == "Annualized Mean Income Growth Bottom 40 Percent (2004-2014)"
        )
        assert (
            get_unit(
                "Barro-Lee: Percentage of female population age 15-19 with no education"
            )
            == "%"
        )
        assert (
            get_unit("MICS: Net attendance rate. Secondary. Male")
            == "MICS: Net attendance rate. Secondary. Male"
        )
        assert (
            get_unit("Mobile account, income, richest 60% (% ages 15+)") == "% ages 15+"
        )
        assert (
            get_unit("Adolescent fertility rate (births per 1,000 women ages 15-19)")
            == "births per 1,000 women ages 15-19"
        )
        assert (
            get_unit("Youth: Neither in School Nor Working  (15-24)")
            == "Youth: Neither in School Nor Working  (15-24)"
        )
        assert (
            get_unit(
                "Average per capita transfer held by 1st quintile (poorest) - Active Labor Market"
            )
            == "Average per capita transfer held by 1st quintile (poorest) - Active Labor Market"
        )
        assert (
            get_unit("Poverty Headcount ($1.90 a day)")
            == "Poverty Headcount ($1.90 a day)"
        )
        assert (
            get_unit("Poverty Severity ($4 a day)-Urban")
            == "Poverty Severity ($4 a day)-Urban"
        )
        assert (
            get_unit("Coverage: Mathematics Proficiency Level 2, Private schools")
            == "Coverage Rate"
        )
        assert get_unit("Mean Log Deviation, GE(0)") == "GE(0)"
        assert get_unit("Mean Log Deviation, GE(0), Rural") == "GE(0), Rural"
        assert (
            get_unit("Number of listed companies per 1,000,000 people ")
            == "listed companies per 1,000,000 people"
        )
        assert get_unit("Number of deaths ages 5-14 years") == "deaths ages 5-14 years"

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        with temp_dir("worldbank") as folder:
            topic = TopicsData.topics[0]
            (
                dataset,
                showcase,
                qc_indicators,
                years,
                rows,
            ) = generate_dataset_and_showcase(
                configuration, downloader, folder, CountriesData.country, topic
            )
            assert dataset == TestWorldBank.dataset
            resource = dataset.get_resources()
            assert resource == TestWorldBank.resources
            filename = f"{slugify(topic['value'])}_{CountriesData.country['iso3']}.csv"
            expected_file = join("tests", "fixtures", filename)
            actual_file = join(folder, filename)
            assert_files_same(expected_file, actual_file)
            filename = (
                f"qc_{slugify(topic['value'])}_{CountriesData.country['iso3']}.csv"
            )
            expected_file = join("tests", "fixtures", filename)
            actual_file = join(folder, filename)
            assert_files_same(expected_file, actual_file)

            assert showcase == {
                "name": "world-bank-gender-and-science-indicators-for-afghanistan-showcase",
                "title": "Gender and Science indicators for Afghanistan",
                "notes": "Gender and Science indicators for Afghanistan",
                "url": "https://data.worldbank.org/topic/gender-and-science?locations=AF",
                "image_url": "https://www.worldbank.org/content/dam/wbr/logo/logo-wb-header-en.svg",
                "tags": [
                    {
                        "name": "economics",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "gender",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "hxl",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "indicators",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
            }
            assert qc_indicators == OtherData.qc_indicators
            assert years == [2016, 2017]
            assert len(rows) == 9

            dataset, _, _, _, topicname = generate_dataset_and_showcase(
                configuration, downloader, folder, CountriesData.madeupcountry, topic
            )
            assert topicname == "Gender and Science"
            character_limit = configuration["character_limit"]
            configuration["character_limit"] = 25
            configuration["indicator_subtract"] = 2
            dataset, _, _, _, _ = generate_dataset_and_showcase(
                configuration, downloader, folder, CountriesData.country, topic
            )
            configuration["character_limit"] = character_limit
            del configuration["indicator_subtract"]
            filename = f"{slugify(topic['value'])}_{CountriesData.country['iso3']}.csv"
            expected_file = join("tests", "fixtures", f"split_{filename}")
            actual_file = join(folder, filename)
            assert_files_same(expected_file, actual_file)

    def test_generate_combined_dataset_and_showcase(self, configuration):
        dataset, showcase, bites_disabled = generate_combined_dataset_and_showcase(
            None, None, CountriesData.madeupcountry, None, None, None, None, None
        )
        assert dataset is None
        assert showcase is None
        assert bites_disabled is None

    def test_generate_all_datasets_showcases(self, configuration, downloader):
        def create_dataset_showcase(dataset, showcase, qc_indicators, batch):
            pass

        with temp_dir("worldbank") as folder:
            dataset, showcase, bites_disabled = generate_all_datasets_showcases(
                configuration,
                downloader,
                folder,
                CountriesData.country,
                TopicsData.topics[:4],
                create_dataset_showcase,
                "1234",
            )
            assert dataset == {
                "name": "world-bank-combined-indicators-for-afghanistan",
                "title": "Afghanistan - Economic, Social, Environmental, Health, Education, Development and Energy",
                "maintainer": "085d3bd8-9035-4b0e-9d2d-80e849dd7b07",
                "owner_org": "905a9a49-5325-4a31-a9d7-147a60a8387c",
                "subnational": "0",
                "groups": [{"name": "afg"}],
                "data_update_frequency": "30",
                "dataset_date": "[2016-01-01T00:00:00 TO 2018-12-31T23:59:59]",
                "tags": [
                    {
                        "name": "economics",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "gender",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "health",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "hxl",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "indicators",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
                "notes": "Contains data from the World Bank's [data portal](http://data.worldbank.org/) covering the following topics which also exist as individual datasets on HDX: [Gender and Science](https://feature.data-humdata-org.ahconu.org/dataset/world-bank-gender-and-science-indicators-for-afghanistan), [Health](https://feature.data-humdata-org.ahconu.org/dataset/world-bank-health-indicators-for-afghanistan).",
            }
            resources = dataset.get_resources()
            assert resources == [
                {
                    "name": "Combined Indicators for Afghanistan",
                    "description": "HXLated csv containing Economic, Social, Environmental, Health, Education, Development and Energy indicators",
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                },
                {
                    "name": "QuickCharts-Combined Indicators for Afghanistan",
                    "description": "Cut down data for QuickCharts",
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                },
            ]
            filename = f"indicators_{CountriesData.country['iso3']}.csv"
            expected_file = join("tests", "fixtures", filename)
            actual_file = join(folder, filename)
            assert_files_same(expected_file, actual_file)
            filename = f"qc_{filename}"
            expected_file = join("tests", "fixtures", filename)
            actual_file = join(folder, filename)
            assert_files_same(expected_file, actual_file)

            assert showcase == {
                "name": "world-bank-combined-indicators-for-afghanistan-showcase",
                "title": "Indicators for Afghanistan",
                "notes": "Economic, Social, Environmental, Health, Education, Development and Energy indicators for Afghanistan",
                "url": "https://data.worldbank.org/?locations=AF",
                "image_url": "https://www.worldbank.org/content/dam/wbr/logo/logo-wb-header-en.svg",
                "tags": [
                    {
                        "name": "economics",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "gender",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "health",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "hxl",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                    {
                        "name": "indicators",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    },
                ],
            }
            assert bites_disabled == [False, True, True]
            dataset, showcase, bites_disabled = generate_all_datasets_showcases(
                configuration,
                downloader,
                folder,
                CountriesData.country,
                [TopicsData.topics[1]],
                create_dataset_showcase,
                "1234",
            )
            assert dataset is None
            assert showcase is None
            assert bites_disabled is None

            with pytest.raises(ValueError):
                _ = generate_all_datasets_showcases(
                    configuration,
                    downloader,
                    folder,
                    CountriesData.country,
                    TopicsData.topics,
                    create_dataset_showcase,
                    "1234",
                )

    def test_generate_topline_dataset(self, configuration, downloader):
        with temp_dir("worldbank") as folder:
            countries = [CountriesData.country, {"iso3": "YYZ"}]
            topline_indicators = configuration["topline_indicators"]
            dataset = generate_topline_dataset(
                configuration["base_url"],
                downloader,
                folder,
                countries,
                topline_indicators,
            )
            assert dataset == {
                "name": "world-bank-country-topline-indicators",
                "title": "Topline Indicators",
                "maintainer": "085d3bd8-9035-4b0e-9d2d-80e849dd7b07",
                "owner_org": "905a9a49-5325-4a31-a9d7-147a60a8387c",
                "subnational": "0",
                "groups": [{"name": "afg"}],
                "data_update_frequency": "30",
                "dataset_date": "[2018-01-01T00:00:00 TO 2018-12-31T23:59:59]",
                "tags": [
                    {
                        "name": "indicators",
                        "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                    }
                ],
            }
            filename = "worldbank_topline.csv"
            expected_file = join("tests", "fixtures", filename)
            actual_file = join(folder, filename)
            assert_files_same(expected_file, actual_file)

            with pytest.raises(ValueError):
                _ = generate_topline_dataset(
                    "http://lala/", downloader, folder, countries, topline_indicators
                )
            with pytest.raises(ValueError):
                _ = generate_topline_dataset(
                    "http://haha/", downloader, folder, countries, topline_indicators
                )
