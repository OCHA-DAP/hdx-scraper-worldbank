#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from datetime import datetime
from os.path import join

from hdx.hdx_configuration import Configuration
from hdx.facades.hdx_scraperwiki import facade
from hdx.utilities.downloader import Download

from worldbank import generate_dataset, get_countries, get_indicators_and_tags, get_dataset_date_range

logger = logging.getLogger(__name__)


indicator_list = 'SP.POP.TOTL,SP.POP.DPND.OL,SP.POP.DPND.YG,SH.DTH.IMRT,SM.POP.TOTL.ZS,AG.LND.TOTL.K2,SP.POP.GROW,' \
                 'EN.POP.DNST,EN.URB.MCTY,NY.GNP.MKTP.CD,SI.POV.DDAY,SP.DYN.AMRT.FE,SP.DYN.AMRT.MA,LP.LPI.OVRL.XQ,' \
                 'IS.RRS.TOTL.KM,NY.GNP.MKTP.PP.CD,NY.GDP.PCAP.PP.CD,NY.GNP.PCAP.PP.CD,DT.ODA.ODAT.PC.ZS,IT.CEL.' \
                 'SETS.P2,NE.CON.PRVT.PP.KD,EG.ELC.ACCS.ZS,SI.POV.GINI,FP.CPI.TOTL.ZG,SN.ITK.DEFC.ZS,IT.MLT.MAIN.P2,' \
                 'NY.GDP.PCAP.PP.KD,SH.DYN.AIDS.ZS,SL.EMP.INSV.FE.ZS'.split(',')


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    downloader = Download()
    indicators, tags = get_indicators_and_tags(base_url, downloader, indicator_list)
    dataset_date_range = get_dataset_date_range(base_url, downloader)

    for countryiso, countryname in get_countries(base_url, downloader):
        dataset = generate_dataset(base_url, countryiso, countryname, indicators, dataset_date_range)
        if dataset is not None:
            logger.info('Adding %s' % countryname)
            dataset.add_tags(tags)
            dataset.update_from_yaml()
            dataset.create_in_hdx()

if __name__ == '__main__':
    facade(main, hdx_site='test', project_config_yaml=join('config', 'project_configuration.yml'))
