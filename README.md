### Collector for World Bank Datasets
[![Run tests](https://github.com/OCHA-DAP/hdx-scraper-worldbank/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-worldbank/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-worldbank/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-worldbank?branch=main)

This script connects to the [World Bank](http://data.worldbank.org/) and extracts data country by country creating around 25 datasets per country in HDX. It makes around 20000 reads from the World Bank API and then 25000 read/writes (API calls) to HDX in a one hour period. It creates around 200 temporary files each under 150Kb. It runs every year. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-worldbank** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, EXTRA_PARAMS, TEMP_DIR, LOG_FILE_ONLY