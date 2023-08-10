import sqlite3
import csv
import os
import pandas as pd
from datetime import datetime
from BlockListParser import BlockListParser
from urllib.parse import urlparse
from publicsuffix2 import PublicSuffixList
from blp_utils import is_js, is_image, get_option_dict


OUTPUT_PATH = os.path.join(".", "output")


##########################################################
############        HELPER FUNCTIONS         #############
##########################################################

def make_list_a_list_of_lists(lst):
    return [[el] for el in lst]
    

def write_file(data, columns, filename):
	df = pd.DataFrame(data=data, columns=columns)
	df.to_csv(os.path.join(OUTPUT_PATH, filename), index=False)

    
##########################################################


##########################################################
############       DATABASE CONNECTION       #############
##########################################################

DATABASE_FOLDER = os.path.join(".", "crawl-data.sqlite")
CONNECTION = sqlite3.connect(DATABASE_FOLDER)
CURSOR = CONNECTION.cursor()
print('DB Connected')
	
def close_db_connection ():
	CONNECTION.close()
	print("DB Closed")
       		
    
##########################################################



##########################################################
############          URL PARSING            #############
##########################################################

def get_domain (url):
    host = urlparse(url).hostname
    domain = psl.get_public_suffix(host)
    return domain
    
    
    
def get_cookie_host_domain (url):
	if url[0] == ".":
		url = url[1:]
	if url [:8] != "https://":
		url = "https://" + url
	return get_domain(url)

##########################################################
            

##########################################################
############  GET STATISTICS ABOUT COOKIES   #############
##########################################################

##################   Static QUERIES    ###################

list_of_visit_ids_and_site_urls = f"SELECT visit_id, site_url FROM site_visits"

##########################################################	

def execute_query_and_fetchall (query):
	return CURSOR.execute(query).fetchall()
	
	
def get_number_of_distinct_cookies_by_website_visit ():
	query = "SELECT site_url, count(name) from (SELECT * FROM javascript_cookies WHERE record_type='added-or-changed' GROUP BY host, name, visit_id ORDER BY visit_id ASC) AS q INNER JOIN site_visits AS sv ON q.visit_id = sv.visit_id GROUP BY q.visit_id"
	return execute_query_and_fetchall(query)
	
	
	
def get_set_of_distinct_cookies_by_visit_id ():
	
	query = "SELECT id, jc.visit_id, site_url, expiry, host, name, value  FROM javascript_cookies AS jc INNER  JOIN site_visits AS sv ON jc.visit_id = sv.visit_id WHERE record_type='added-or-changed' AND jc.visit_id={VISIT_ID} GROUP BY host, name ORDER BY name ASC"
	
	results = []
	tmp_visit_id = "{VISIT_ID}"
	visit_ids = execute_query_and_fetchall(list_of_visit_ids_and_site_urls)
	for visit_id in visit_ids:
		query = query.replace(tmp_visit_id, str(visit_id[0]))
		results.extend(execute_query_and_fetchall(query))
		tmp_visit_id = str(visit_id[0])
		
	return results
	

# apply query on every visit_id-site_url pair and return result
def iterate_query_over_visit_ids_and_site_urls (query):
	
	results = []
	tmp_visit_id = "{VISIT_ID}"
	tmp_site_url = "{SITE_URL}"
	visit_ids_and_site_urls = execute_query_and_fetchall(list_of_visit_ids_and_site_urls)
	
	for item in visit_ids_and_site_urls:
		visit_id = str(item[0])
		site_url = get_domain(str(item[1])).split(".")[0]	# url of the tested site
		
		query = query.replace(tmp_visit_id, visit_id)
		query = query.replace(tmp_site_url, site_url)
		results.extend(execute_query_and_fetchall(query))
		tmp_visit_id = visit_id
		tmp_site_url = site_url	
		
	return results
	
	
def get_set_of_distinct_first_party_cookies_by_visit_id_and_site_url ():
	
	query = "SELECT id, jc.visit_id, site_url, expiry, host, name, value  FROM javascript_cookies AS jc INNER  JOIN site_visits AS sv ON jc.visit_id = sv.visit_id WHERE record_type='added-or-changed' AND jc.visit_id={VISIT_ID} AND host LIKE '%{SITE_URL}%' GROUP BY host, name ORDER BY name ASC"
	
	
	return iterate_query_over_visit_ids_and_site_urls(query)
	
	
	
def get_count_of_distinct_first_party_cookies_by_visit_id_and_site_url ():
	query = "SELECT site_url, count(id) from (SELECT * FROM javascript_cookies AS jc INNER  JOIN site_visits AS sv ON jc.visit_id = sv.visit_id WHERE record_type='added-or-changed' AND jc.visit_id={VISIT_ID} AND host LIKE '%{SITE_URL}%' GROUP BY host, name ORDER BY name ASC)"
	
	
	return iterate_query_over_visit_ids_and_site_urls(query)
	


def get_set_of_distinct_third_party_cookies_by_visit_id_and_site_url ():
	query = "SELECT id, jc.visit_id, site_url, expiry, host, name, value  FROM javascript_cookies AS jc INNER  JOIN site_visits AS sv ON jc.visit_id = sv.visit_id WHERE record_type='added-or-changed' AND jc.visit_id={VISIT_ID} AND host NOT LIKE '%{SITE_URL}%' GROUP BY host, name ORDER BY name ASC"
	
	return iterate_query_over_visit_ids_and_site_urls(query)
	


def get_count_of_distinct_third_party_cookies_by_visit_id_and_site_url ():
	query = "SELECT site_url, count(id) from (SELECT * FROM javascript_cookies AS jc INNER  JOIN site_visits AS sv ON jc.visit_id = sv.visit_id WHERE record_type='added-or-changed' AND jc.visit_id={VISIT_ID} AND host NOT LIKE '%{SITE_URL}%' GROUP BY host, name ORDER BY name ASC)"
	
	return iterate_query_over_visit_ids_and_site_urls(query)




def get_count_of_distinct_first_and_third_party_cookies_by_visit_id_and_site_url ():
	query = "SELECT c1.site_url, count(id) as 'first_party_cookies', third_party_cookies from (SELECT * FROM javascript_cookies AS jc INNER  JOIN site_visits AS sv ON jc.visit_id = sv.visit_id WHERE record_type='added-or-changed' AND jc.visit_id={VISIT_ID} AND host LIKE '%{SITE_URL}%' GROUP BY host, name ORDER BY name ASC) AS c1 INNER JOIN (SELECT site_url, count(id) as 'third_party_cookies' from (SELECT * FROM javascript_cookies AS jc INNER  JOIN site_visits AS sv ON jc.visit_id = sv.visit_id WHERE record_type='added-or-changed' AND jc.visit_id={VISIT_ID} AND host NOT LIKE '%{SITE_URL}%' GROUP BY host, name ORDER BY name ASC)) AS c2 ON c1.site_url = c2.site_url"
	
	return iterate_query_over_visit_ids_and_site_urls(query)
	


def get_overall_frequency_of_third_party_cookies ():
	third_party_cookies = get_set_of_distinct_third_party_cookies_by_visit_id_and_site_url()
	
	unique_third_party_cookie_domains = set()
	repeated_third_party_cookie_domains = []
	# cookie structure ([0]id, [1]visit_id, [2]site_url, [3]expiry, [4]host, [5]name, [6]value)
	for cookie in third_party_cookies:
		domain = get_cookie_host_domain(cookie[4])
		if domain is not None:
			unique_third_party_cookie_domains.add(domain)
			repeated_third_party_cookie_domains.append(domain)
		
	cookie_count_list = []
	for unique_domain in unique_third_party_cookie_domains:
		counter = 0
		for domain in repeated_third_party_cookie_domains:
			if unique_domain == domain:
				counter += 1
		cookie_count_list.append([unique_domain, counter])

		
	return cookie_count_list
	
def get_expiry_dates_statistics_for_every_third_party_cookie_host_domain ():
	third_party_cookie_count_list = get_overall_frequency_of_third_party_cookies()
	
	third_party_cookies = get_set_of_distinct_third_party_cookies_by_visit_id_and_site_url()
	expiry_dates_dict = {}
	for cookie in third_party_cookie_count_list:
		expiry_dates_dict[cookie[0]] = []
	# cookie structure ([0]id, [1]visit_id, [2]site_url, [3]expiry, [4]host, [5]name, [6]value)
	for cookie in third_party_cookies:
		domain = get_cookie_host_domain(cookie[4])
		expiry_date = cookie[3]
		expiry_dates_dict[domain].append(expiry_date)
		
	
	third_party_cookie_count_and_expiry_count_list = []
	crawl_date = execute_query_and_fetchall("SELECT start_time FROM crawl")[0][0]
	crawl_date = crawl_date.split(" ")[0] + "T" + crawl_date.split(" ")[1]
	crawl_date = datetime.strptime(crawl_date, "%Y-%m-%dT%H:%M:%S")
	for cookie in third_party_cookie_count_list:
		expiry_dates = expiry_dates_dict[cookie[0]]
			
		g_1_month_leq_1_year = 0
		g_1_year_leq_5_years = 0
		g_5_years = 0
		year_eq_to_9999 = 0
		
		for expiry_date in expiry_dates:
			expiry_date = expiry_date.split(".")[0]	# remove the .000Z part
			expiry_date = datetime.strptime(expiry_date, "%Y-%m-%dT%H:%M:%S")
			
			duration = expiry_date - crawl_date
			seconds = duration.total_seconds()
			days  = seconds/(24*60*60)
			months = days/30
			years = days/365
			
			if months > 1 and years <= 1:
				g_1_month_leq_1_year += 1
			if years > 1 and years <= 5:
				g_1_year_leq_5_years += 1
			if years > 5:
				g_5_years += 1
			if str(expiry_date)[:4] == "9999":
				year_eq_to_9999 += 1
		
		third_party_cookie_count_and_expiry_count_list.append([cookie[0], cookie[1], g_1_month_leq_1_year, g_1_year_leq_5_years, g_5_years, year_eq_to_9999])
	
	return third_party_cookie_count_and_expiry_count_list
				
			
			
			
			
	
	
	

##########################################################



##########################################################
#####   CATEGORIZE COOKIES AND JAVASCRIPT FILES    #######
##########################################################

TRACKING = 'tracking'
ADVERTISE = 'advertise'
UNKNOWN = 'unknown'

psl = PublicSuffixList()

advertisers = [
    BlockListParser('./rules/Easylist.txt'),
    BlockListParser('./rules/Easylist_China.txt')
]
script_trackers = [
    BlockListParser('./rules/EasyPrivacy.txt'),
    BlockListParser('./rules/Fanboys_Annoyance_List.txt'),
    BlockListParser('./rules/Fanboys_SocialBlocking_List.txt')
]
cookies_trackers = script_trackers
cookies_trackers.append(
    BlockListParser('./rules/Easylist_Cookie_List.txt')
)

def get_distinct_third_party_scripts_by_site ():
	visit_ids_and_site_urls = execute_query_and_fetchall(list_of_visit_ids_and_site_urls)
	
	third_party_scripts = []
	for item in visit_ids_and_site_urls:
		visit_id = item[0]
		site_url = item[1]
		site_url_domain = get_domain(site_url)
		site_url_domain_name = site_url_domain.split(".")[0]
		query = f"SELECT DISTINCT script_url FROM javascript WHERE visit_id='{visit_id}' AND script_url NOT LIKE '%{site_url_domain_name}%' AND LENGTH(script_url)>0"
		scripts = execute_query_and_fetchall(query)
		for script in scripts:
			item = {}
			item['fp_url'] = "https://www." + site_url_domain
			item['tp_script_domain'] = get_domain(script[0])
			item['script'] = script[0]
			third_party_scripts.append(item)
			
	return third_party_scripts
		
	
		

def categorize_third_party_scripts():

	result = {ADVERTISE: [], TRACKING: [], UNKNOWN: []}
	scripts_list = get_distinct_third_party_scripts_by_site()
	advertisement_scripts_by_fp_url = {}
	trackers_scripts_by_fp_url = {}
	unknown_scripts_by_fp_url = {}
	distinct_sites = set()
	for item in scripts_list:
		script, fp_url = item['script'], item['fp_url']
		content_type = 'application/javascript'
		distinct_sites.add(fp_url)
		options = get_option_dict(script, fp_url, is_js(script, content_type), is_image(script, content_type), psl)
		if advertisers[0].should_block(script, options) or advertisers[1].should_block(script, options):
			try:
				advertisement_scripts_by_fp_url[fp_url].append(script)
			except:
				advertisement_scripts_by_fp_url[fp_url] = []
				advertisement_scripts_by_fp_url[fp_url].append(script)
		elif script_trackers[0].should_block(script, options) or script_trackers[1].should_block(script, options):
			try:
				trackers_scripts_by_fp_url[fp_url].append(script)
			except:
				trackers_scripts_by_fp_url[fp_url] = []
				trackers_scripts_by_fp_url[fp_url].append(script)
		else:
			try:
				unknown_scripts_by_fp_url[fp_url].append(script)
			except:
				unknown_scripts_by_fp_url[fp_url] = []
				unknown_scripts_by_fp_url[fp_url].append(script)

	# list structure: site, advertisers, trackers, unknown
	list_of_count_of_scripts_by_site_and_category = []
	total_adv_count = 0
	total_trackers_count = 0
	total_unknown_count = 0
	for site in distinct_sites:
		adv_count = 0 
		trackers_count = 0 
		unknown_count = 0 
		try:
			adv_count = len(advertisement_scripts_by_fp_url[site])
			total_adv_count += adv_count
		except:
			pass
		try:
			trackers_count = len(trackers_scripts_by_fp_url[site])
			total_trackers_count += trackers_count
		except:
			pass
		try:
			unknown_count = len(unknown_scripts_by_fp_url[site])
			total_unknown_count += unknown_count
		except:
			pass
		
		list_of_count_of_scripts_by_site_and_category.append([site, adv_count, trackers_count, unknown_count])


	# list structure: site, third_party_domain, script, category
	list_of_categorized_scripts = []
	for site in advertisement_scripts_by_fp_url.keys():
		for script in advertisement_scripts_by_fp_url[site]:
			list_of_categorized_scripts.append([site, get_domain(script), script, "ADVERTISER"])
	for site in trackers_scripts_by_fp_url.keys():
		for script in trackers_scripts_by_fp_url[site]:
			list_of_categorized_scripts.append([site, get_domain(script), script, "TRACKER"])
	for site in unknown_scripts_by_fp_url.keys():
		for script in unknown_scripts_by_fp_url[site]:
			list_of_categorized_scripts.append([site, get_domain(script), script, "UNKOWN"])


	write_file(list_of_count_of_scripts_by_site_and_category, ["site", "advertisers", "trackers", "unknown"], "count_of_scripts_by_site_and_category.csv")
	write_file(list_of_categorized_scripts, ["site", "third_party_domain", "script", "category"], "list_of_categorized_scripts.csv")
	write_file([["ADVERTISER",total_adv_count], ["TRACKER",total_trackers_count], ["UNKOWN",total_unknown_count]], ["category", "count"],  "total_count_of_categorized_scripts.csv")


def categorize_third_party_cookies ():
	result = {ADVERTISE: [], TRACKING: [], UNKNOWN: []}
	cookies_list = get_set_of_distinct_third_party_cookies_by_visit_id_and_site_url()
	advertisement_cookies_by_fp_url = {}
	trackers_cookies_by_fp_url = {}
	unknown_cookies_by_fp_url = {}
	distinct_sites = set()
	for cookie in cookies_list:
		fp_url = cookie[2]
		tp_url = "https://" + get_cookie_host_domain(cookie[4])
		content_type = 'application/javascript'
		distinct_sites.add(fp_url)
		options = get_option_dict(tp_url, fp_url, True, False, psl)
		
		if advertisers[0].should_block(tp_url, options) or advertisers[1].should_block(tp_url, options):
			try:
				advertisement_cookies_by_fp_url[fp_url].append(tp_url)
			except:
				advertisement_cookies_by_fp_url[fp_url] = []
				advertisement_cookies_by_fp_url[fp_url].append(tp_url)
		elif  cookies_trackers[0].should_block(tp_url, options) or cookies_trackers[1].should_block(tp_url, options) or \
			cookies_trackers[2].should_block(tp_url, options):
			try:
					trackers_cookies_by_fp_url[fp_url].append(tp_url)
			except:
				trackers_cookies_by_fp_url[fp_url] = []
				trackers_cookies_by_fp_url[fp_url].append(tp_url)
		
		else:
			try:
				unknown_cookies_by_fp_url[fp_url].append(tp_url)
			except:
				unknown_cookies_by_fp_url[fp_url] = []
				unknown_cookies_by_fp_url[fp_url].append(tp_url)

	# list structure: site, advertisers, trackers, unknown
	list_of_count_of_cookies_by_site_and_category = []
	total_adv_count = 0
	total_trackers_count = 0
	total_unknown_count = 0
	for site in distinct_sites:
		adv_count = 0 
		trackers_count = 0 
		unknown_count = 0 
		try:
			adv_count = len(advertisement_cookies_by_fp_url[site])
			total_adv_count += adv_count
		except:
			pass
		try:
			trackers_count = len(trackers_cookies_by_fp_url[site])
			total_trackers_count += trackers_count
		except:
			pass
		try:
			unknown_count = len(unknown_cookies_by_fp_url[site])
			total_unknown_count += unknown_count
		except:
			pass
		
		list_of_count_of_cookies_by_site_and_category.append([site, adv_count, trackers_count, unknown_count])


	# list structure: site, third_party_domain, category
	list_of_categorized_cookies = []
	for site in advertisement_cookies_by_fp_url.keys():
		for tp_url in advertisement_cookies_by_fp_url[site]:
			list_of_categorized_cookies.append([site, get_domain(tp_url), "ADVERTISER"])
	for site in trackers_cookies_by_fp_url.keys():
		for tp_url in trackers_cookies_by_fp_url[site]:
			list_of_categorized_cookies.append([site, get_domain(tp_url), "TRACKER"])
	for site in unknown_cookies_by_fp_url.keys():
		for tp_url in unknown_cookies_by_fp_url[site]:
			list_of_categorized_cookies.append([site, get_domain(tp_url), "UNKOWN"])


	write_file(list_of_count_of_cookies_by_site_and_category, ["site", "advertisers", "trackers", "unknown"], "count_of_cookies_by_site_and_category.csv")
	write_file(list_of_categorized_cookies, ["site", "third_party_domain", "category"], "list_of_categorized_cookies.csv")
	write_file([["ADVERTISER",total_adv_count], ["TRACKER",total_trackers_count], ["UNKOWN",total_unknown_count]], ["category", "count"],  "total_count_of_categorized_cookies.csv")



##########################################################

  
##########################################################
###################    MAIN FUNCTION    ##################
########################################################## 
if __name__ == "__main__":
	
	# operation 1
	write_file(get_number_of_distinct_cookies_by_website_visit(), ["site_url", "count"], "number_of_distinct_cookies_by_website_visit.csv")
	
	# operation 2
	write_file(get_set_of_distinct_cookies_by_visit_id(), ["id", "visit_id", "site_url", "expiry", "host", "name", "value"], "set_of_distinct_cookies_by_visit_id.csv")
	
	# operation 3
	write_file(get_set_of_distinct_first_party_cookies_by_visit_id_and_site_url(), ["id", "visit_id", "site_url", "expiry", "host", "name", "value"], "set_of_distinct_first_party_cookies_by_visit_id_and_site_url.csv")
	
	# operation 4
	write_file(get_count_of_distinct_first_party_cookies_by_visit_id_and_site_url(), ["site_url", "count"],  "count_of_distinct_first_party_cookies_by_visit_id_and_site_url.csv")
	
	# operation 5
	write_file(get_set_of_distinct_third_party_cookies_by_visit_id_and_site_url(), ["id", "visit_id", "site_url", "expiry", "host", "name", "value"], "set_of_distinct_third_party_cookies_by_visit_id_and_site_url.csv")
	
	# operation 6
	write_file(get_count_of_distinct_third_party_cookies_by_visit_id_and_site_url(), ["site_url", "count"], "count_of_distinct_third_party_cookies_by_visit_id_and_site_url.csv")
	
	# operation 7
	write_file(get_count_of_distinct_first_and_third_party_cookies_by_visit_id_and_site_url(), ["site_url", "first_party_cookies", "third_party_cookies"], "count_of_distinct_third_and_first_party_cookies_by_visit_id_and_site_url.csv")
	
	# operation 7
	write_file(get_overall_frequency_of_third_party_cookies(), ["third_party_cookie_host", "number_of_occurences"], "overall_frequency_of_third_party_cookies.csv")
	
	# operation 8
	write_file(get_expiry_dates_statistics_for_every_third_party_cookie_host_domain(), ["third_party_cookie_host", "number_of_occurences", ">1m & <=1y", ">1y & <=5y", ">5y", "y=9999"],  "expiry_dates_statistics_for_every_third_party_cookie_host_domain.csv")
	
	# operation 9
	categorize_third_party_scripts()
	
	# operation 10
	categorize_third_party_cookies()
	
	close_db_connection()
	
	

