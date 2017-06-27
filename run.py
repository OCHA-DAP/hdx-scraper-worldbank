#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions
that register datasets in HDX.

'''
import sys
import logging
from os.path import join
from tempfile import gettempdir

from hdx.configuration import Configuration
from hdx.facades.hdx_scraperwiki import facade
from hdx.utilities.downloader import Download

from worldbank import generate_dataset, get_countries, get_indicators_and_tags

logger = logging.getLogger(__name__)


indicator_list = [
    'SP.POP.TOTL',
    'SP.POP.DPND.OL',
    'SP.POP.DPND.YG',
    'SH.DTH.IMRT',
    'SM.POP.TOTL.ZS',
    'AG.LND.TOTL.K2',
    'SP.POP.GROW',
    'EN.POP.DNST',
    'EN.URB.MCTY',
    'NY.GNP.MKTP.CD',
    'SI.POV.DDAY',
    'SP.DYN.AMRT.FE',
    'SP.DYN.AMRT.MA',
    'LP.LPI.OVRL.XQ',
    'IS.RRS.TOTL.KM',
    'NY.GNP.MKTP.PP.CD',
    'NY.GDP.PCAP.PP.CD',
    'NY.GNP.PCAP.PP.CD',
    'DT.ODA.ODAT.PC.ZS',
    'IT.CEL.SETS.P2',
    'NE.CON.PRVT.PP.KD',
    'EG.ELC.ACCS.ZS',
    'SI.POV.GINI',
    'FP.CPI.TOTL.ZG',
    'SN.ITK.DEFC.ZS',
    'IT.MLT.MAIN.P2',
    'NY.GDP.PCAP.PP.KD',
    'SH.DYN.AIDS.ZS',
    'SL.EMP.INSV.FE.ZS'
]


def main():
    '''Generate dataset and create it in HDX'''

    base_url = Configuration.read()['base_url']
    downloader = Download()
    indicators, tags = get_indicators_and_tags(base_url, downloader, indicator_list)

    for countryiso, countryname in get_countries(base_url, downloader):
        dataset = generate_dataset(countryiso, countryname, indicators)
        if dataset is not None:
            logger.info('Adding %s' % countryname)
            dataset.add_tags(tags)
            dataset.update_from_yaml()
            dataset.create_in_hdx()

if __name__ == '__main__':
    facade(main, hdx_site='test', project_config_yaml=join('config', 'project_configuration.yml'))
