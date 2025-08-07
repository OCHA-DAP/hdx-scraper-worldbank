class TopicsData:
    gender = [
        {
            "id": "SH.STA.MMRT",
            "name": "Maternal mortality ratio (modeled estimate, per 100, 000 live births)",
            "unit": "",
            "source": {"id": "2", "value": "World Development Indicators"},
            "sourceNote": "Maternal mortality ratio is ...",
            "sourceOrganization": "WHO, UNICEF, UNFPA, World Bank Group, and the United Nations Population Division. Trends in Maternal Mortality:  2000 to 2017. Geneva, World Health Organization, 2019",
            "topics": [
                {"id": "8", "value": "Health "},
                {"id": "17", "value": "Gender"},
                {"id": "2", "value": "Aid Effectiveness "},
            ],
        },
        {
            "id": "SG.LAW.CHMR",
            "name": "Law prohibits or invalidates child or early marriage (1=yes; 0=no)",
            "unit": "",
            "source": {"id": "2", "value": "World Development Indicators"},
            "sourceNote": "Law prohibits or invalidates...",
            "sourceOrganization": "World Bank: Women, Business and the Law.",
            "topics": [
                {"id": "13", "value": "Public Sector "},
                {"id": "17", "value": "Gender"},
            ],
        },
        {
            "id": "SP.ADO.TFRT",
            "name": "Adolescent fertility rate (births per 1,000 women ages 15-19)",
            "unit": "",
            "source": {"id": "2", "value": "World Development Indicators"},
            "sourceNote": "Adolescent fertility rate is...",
            "sourceOrganization": "United Nations Population Division,  World Population Prospects.",
            "topics": [
                {"id": "8", "value": "Health "},
                {"id": "17", "value": "Gender"},
                {"id": "15", "value": "Social Development "},
            ],
        },
        {
            "id": "SH.MMR.RISK",
            "name": "Lifetime risk of maternal death (1 in: rate varies by country)",
            "unit": "",
            "source": {"id": "2", "value": "World Development Indicators"},
            "sourceNote": "Life time risk of maternal death is...",
            "sourceOrganization": "WHO, UNICEF, UNFPA, World Bank Group, and the United Nations Population Division. Trends in Maternal Mortality:  2000 to 2017. Geneva, World Health Organization, 2019",
            "topics": [
                {"id": "8", "value": "Health "},
                {"id": "17", "value": "Gender"},
            ],
        },
    ]
    poverty = [
        {
            "id": "SI.POV.GAPS",
            "name": "Poverty gap at $1.90 a day (2011 PPP) (%)",
            "unit": "",
            "source": {"id": "2", "value": "World Development Indicators"},
            "sourceNote": "Poverty gap at $1.90 a day (2011 PPP)..",
            "sourceOrganization": "World Bank, Development Research Group.",
            "topics": [{"id": "11", "value": "Poverty "}],
        }
    ]
    health = [
        {
            "id": "SP.POP.TOTL",
            "name": "Population, total",
            "unit": "",
            "source": {"id": "2", "value": "World Development Indicators"},
            "sourceNote": "Total population is based on the de facto definition of population..",
            "sourceOrganization": "(1) United Nations Population Division. World Population Prospects: 2019 Revision...",
            "topics": [
                {"id": "11", "value": "Climate Change"},
                {"id": "8", "value": "Health "},
            ],
        }
    ]
    population = [
        {
            "id": "SP2.POP.TOTL",
            "name": "Population2, total",
            "unit": "",
            "source": {"id": "2", "value": "World Development Indicators"},
            "sourceNote": "Total population is based on the de facto definition of population..",
            "sourceOrganization": "(1) United Nations Population Division. World Population Prospects: 2019 Revision...",
            "topics": [
                {"id": "11", "value": "Climate Change"},
                {"id": "8", "value": "Health "},
            ],
        }
    ]
    economics = [
        {
            "id": "XX.YYY.ZZZZ",
            "name": "Economics",
            "unit": "",
            "source": {"id": "5", "value": "Something"},
            "sourceNote": "Something..",
            "sourceOrganization": "Someone...",
            "topics": [{"id": "99", "value": "Economics"}],
        },
        {
            "id": "IGNORE.ME",
            "name": "Economics",
            "unit": "",
            "source": {"id": "2", "value": "Something"},
            "sourceNote": "Something..",
            "sourceOrganization": "Someone...",
            "topics": [{"id": "99", "value": "Economics"}],
        },
    ]

    topics = [
        {
            "id": "17",
            "value": "Gender and Science",
            "sourceNote": "Gender equality is a core development objective...",
            "tags": ["gender", "science"],
            "sources": {"2": gender},
        },
        {
            "id": "11",
            "value": "Poverty",
            "sourceNote": "For countries with an active poverty monitoring program...",
            "tags": ["poverty"],
            "sources": {"2": poverty},
        },
        {
            "id": "8",
            "value": "Health",
            "sourceNote": "Improving health is central to the Millennium Development Goals...",
            "tags": ["health"],
            "sources": {"2": health},
        },
        {
            "id": "99",
            "value": "Economics",
            "sourceNote": "Something...",
            "tags": ["economics"],
            "sources": {},
        },
        {
            "id": "95",
            "value": "Population",
            "sourceNote": "Improving health is central to the Millennium Development Goals...",
            "tags": ["population"],
            "sources": {"2": population},
        },
    ]
