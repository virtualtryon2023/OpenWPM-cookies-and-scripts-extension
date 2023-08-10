# OpenWPM-cookies-and-scripts-extension

This repository contains the openWPM extension that was used in the study for the measurement of third-party tracking cookies and scripts. Our extension allows us to get the following information about the crawled websites:

(1) the number (and details) of distinct first-party and third-party cookies per website

(2) the overall number of occurrences of third-party cookies across the list of websites

(3) the expiry dates statistics for every third-party cookie host domain

(4) the categorization of third-party cookies across websites

(5) the categorization of third-party scripts across websites

## Usage
* `output` folder must be created if not there
* run openwpm to get the sqlite file containing the crawl data
* rename sqlite file containing crawl data to `crawl-data.sqlite`, it should be placed in the same directory as `analyse_cookies_scripts.py`
* run `python analyse_cookies_scripts.py`
* see `output` folder for output files

**NOTE:** if running the tool on Windows, add `encoding="utf-8-sig"` option to the `open()` function in `line 15` of the file `BlockListParser.py`.

## Description of Folders and Files
### Folders
* output : output files will be saved in this folder (it currently contains sample output)
* rules  : contains the rules from EasyList and EasyPrivacy lists that are used to match against URLs

### Main File
* analyse_cookies_scripts.py : the extension developed by the authors and the entry point of the program.

### Helper Files
These files were not developed by the authors of the study. It is originally by [Englehardt](https://github.com/englehardt/abp-blocklist-parser/tree/master).
* BlockListParser.py
* FastHash.py
* RegexParser.py

This file has an unknown author:
* blp_utils.py

