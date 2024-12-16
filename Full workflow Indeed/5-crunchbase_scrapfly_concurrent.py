import subprocess
import sys
import os

def install(package):
    """
    Install a package using pip.
    
    Args:
        package (str): The name of the package to install.
    """
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def ensure_packages_installed(packages):
    """
    Ensure all required packages are installed. If a package is not installed,
    it will be installed.

    Args:
        packages (list): A list of package names to check and install if missing.
    """
    for package in packages:
        try:
            __import__(package)
        except ImportError:
            print(f"Installing {package}...")
            install(package)

# List of required packages
required_packages = ["parsel", "scrapfly-sdk", "httpx"]

# Ensure all required packages are installed
ensure_packages_installed(required_packages)

import gzip
import json
import httpx
import asyncio
import pandas as pd
import jmespath
import numpy as np
from datetime import datetime, date
from typing import Dict, List, TypedDict, Iterator, Literal, Tuple
from loguru import logger as log
from parsel import Selector
from dotenv import load_dotenv
from scrapfly import ScrapflyClient, ScrapeConfig, ScrapeApiResponse

log.info("Scraping Crunchbase data using Scrapfly")

load_dotenv()
api_key = os.environ["scrapfly_key"]
#dotenv_path = os.path.join(sys.path[0], os.path.pardir, "Saudi", ".env")
#api_key = dotenv_values(dotenv_path)["scrapfly_key"]

scrapfly = ScrapflyClient(key=api_key,  max_concurrency=5)

BASE_CONFIG = {
    "asp": True,
    "proxy_pool":"public_residential_pool",
    "render_js": True,
    "country":"us"
}
rows=[]
rows2=[]
errors=[]

def extract_company_name(url):
    """
    This function retrieves organization name from an url.
    
    Args:
        url (str): The crunchbase url of a company.

    Returns:
        str: The company name extracted from the url.
    """
    try:
        return_string = str(url).split('organization/')[-1]
        if return_string[0] !='%':
            return return_string
        else:
            return np.nan
    except:
        return np.nan


class CompanyData(TypedDict):
    """
    Type hint for data returned by Crunchbase company page parser.

    Attributes:
        organization (Dict): Parsed organization details.
        employees (List[Dict]): List of parsed employee details.
    """

    organization: Dict
    employees: List[Dict]


def _parse_organization_data(data: Dict) -> Dict:
    """
    Parse main company details from the whole company dataset.

    Args:
        data (Dict): The raw data from Crunchbase.

    Returns:
        Dict: Parsed company details.
    """
    properties = data['properties']
    cards = data['cards']
    parsed = {
        # theres meta data in the properties field:
        "name": properties['title'],
        "id": properties['identifier']['permalink'],
        "logo": "https://res.cloudinary.com/crunchbase-production/image/upload/" + properties['identifier']['image_id'],
        "description": properties['short_description'],
        # but most of the data is in the cards field:
        "semrush_global_rank": cards['semrush_summary']['semrush_global_rank'],
        "semrush_visits_latest_month": cards['semrush_summary']['semrush_visits_latest_month'],
        # etc... There's much more data!
    }
    return parsed

def _parse_employee_data(data: Dict) -> List[Dict]:
    """
    Parse employee details from the whole employee dataset.

    Args:
        data (Dict): The raw data from Crunchbase.

    Returns:
        List[Dict]: Parsed employee details.
    """
    parsed = []
    for person in data['entities']:
        parsed.append({
            "name": person['properties']['name'],
            "linkedin": person['properties'].get('linkedin'),
            "job_levels": person['properties'].get('job_levels'),
            "job_departments": person['properties'].get('job_departments'),
            # etc...
        })
    return parsed

def _unescape_angular(text):
    """
    Helper function to unescape Angular quoted text.

    Args:
        text (str): The text to unescape.

    Returns:
        str: The unescaped text.
    """
    ANGULAR_ESCAPE = {
        "&a;": "&",
        "&q;": '"',
        "&s;": "'",
        "&l;": "<",
        "&g;": ">",
    }
    for from_, to in ANGULAR_ESCAPE.items():
        text = text.replace(from_, to)
    return text


def parse_company(result: ScrapeApiResponse) -> CompanyData:
    """
    Parse company page for company and employee data.

    Args:
        result (ScrapeApiResponse): The response from the scrape.

    Returns:
        CompanyData: Parsed company and employee data.
    """
    # the app cache data can be in one of two places:
    app_state_data = result.selector.css("script#ng-state::text").get()
    if not app_state_data:
        app_state_data = _unescape_angular(result.selector.css("script#client-app-state::text").get() or "")
    app_state_data = json.loads(app_state_data)
    
    # there are multiple caches:
    cache_keys = list(app_state_data["HttpState"])
    
    # Organization data can be found in this cache:
    data_cache_key = next(key for key in cache_keys if "entities/organizations/" in key)
    
    # Some employee/contact data can be found in this key:
    try:
        people_cache_key = next(key for key in cache_keys if "/data/searches/contacts" in key)
    except StopIteration:
        people_cache_key = None

    organization = app_state_data["HttpState"][data_cache_key]["data"]
    if people_cache_key is not None:
        employees = app_state_data["HttpState"][people_cache_key]["data"]
    else:
        employees = {"entities":[]}
    return {
        "organization": _reduce_organization_dataset(organization),
        "employees": _reduce_employee_dataset(employees),
    }


class PersonData(TypedDict):
    """
    Type hint for data returned by person page parser.

    Attributes:
        id (str): The person's ID.
        name (str): The person's name.
    """
    id: str
    name: str


def parse_person(result: ScrapeApiResponse) -> Dict:
    """
    Parse person page for personal data.

    Args:
        result (ScrapeApiResponse): The response from the scrape.

    Returns:
        Dict: Parsed personal data.
    """
    app_state_data = result.selector.css("script#ng-state::text").get()
    if not app_state_data:
        app_state_data = _unescape_angular(result.selector.css("script#client-app-state::text").get() or "")
    app_state_data = json.loads(app_state_data)
    cache_keys = list(app_state_data["HttpState"])
    dataset_key = next(key for key in cache_keys if "data/entities" in key)
    dataset = app_state_data["HttpState"][dataset_key]["data"]
    return _reduce_person_dataset(dataset)


async def scrape_person(person_id: str) -> Dict:
    """
    Scrape personal data from Crunchbase person page.

    Args:
        person_id (str): The person's ID.

    Returns:
        Dict: Parsed personal data.
    """
    url = f"https://www.crunchbase.com/person/{person_id}"
    result = await scrapfly.async_scrape(ScrapeConfig(url, **BASE_CONFIG))
    return parse_person(result)

async def scrape_company(company_url: str) -> CompanyData:
    """
    Scrape Crunchbase company page for organization and employee data.

    Args:
        company_url (str): The URL of the company page.

    Returns:
        CompanyData: Parsed company and employee data.
    """
    
    # note: we use /people tab because it contains the most data:
    print("scrap company: ", company_url.split('/')[-1])
    url = f"{company_url}/people"
    async for result in scrapfly.concurrent_scrape([ScrapeConfig(url, **BASE_CONFIG)]):

        try:
            app_state_data = result.selector.css("script#ng-state::text").get()
            if not app_state_data:
                app_state_data = _unescape_angular(result.selector.css("script#client-app-state::text").get() or "")
            app_state_data = json.loads(app_state_data)
        except Exception as e:
            print(e)
            errors.append({"msg":e, "urls":url})
            continue;
        
        # there are multiple caches:
        try:
            cache_keys = list(app_state_data["HttpState"])
        except KeyError:
            errors.append({"msg":"HttpState key not found", "urls":url})
            continue;

        # Organization data can be found in this cache:
        data_cache_key = next(key for key in cache_keys if "entities/organizations/" in key)
        
        # Some employee/contact data can be found in this key:
        people_cache_key = next((key for key in cache_keys if "/data/searches/contacts" in key), None)
        if people_cache_key is None:
            errors.append({"msg":"No employee information", "urls":url})
            continue;

        organization = app_state_data["HttpState"][data_cache_key]["data"]
        employees = app_state_data["HttpState"][people_cache_key]["data"]

        data= {
            "organization": _reduce_organization_dataset(organization),
            "employees": _reduce_employee_dataset(employees),   }

        try:
            name=data["organization"]["name"] 
            semrush=data["organization"]["semrush"]
            email=data["organization"]["email"]
            phone=data["organization"]["phone"]
            ipo=data["organization"]["ipo_status"]
            

            for d in data["employees"]:
                people=d["name"]
                levels=d["job_levels"]
                departments=d["job_departments"]
                title=d["job_title"]
                li="https://www.linkedin.com/in/"+ d["linkedin"]
                nr={'Company ID':semrush,'Company Name': name, 'Name from URL': extract_company_name(company_url), 'People Name': people, 'Linkedin Profile': li,
                        'Job Title': title, 'Job Category': levels, 'Departments': departments}
                
                rows.append(nr)


            desc= data["organization"]["description"]
            cat=data["organization"]["categories"]
            foundon=data["organization"]["founded_on"]
            city=data["organization"]["city"]
            state=data["organization"]["state"]
            country=data["organization"]["country"]
            size=data["organization"]["size"]
            about=data["organization"]["about"]
            lin=data["organization"]["linkedin"]
            fb=data["organization"]["facebook"]
            twt=data["organization"]["twitter"]
            similar=data["organization"]["similar"]
            invs=data["organization"]["investors"]
            fr=data["organization"]["funding_rounds"]
            ft_usd=data["organization"]["funding_usd"]
            aqu=data["organization"]["acquisitions"]
            inve=data["organization"]["investments"]
            lis=data["organization"]["lead_investments"]
            inve2=data["organization"]["lead_investors"]
            tpa=data["organization"]["total_products_active"]
            atc=data["organization"]["active_tech_count"]
            mvisits=data["organization"]["semrush_visits_latest_month"]
            mvisits_change=data["organization"]["semrush_visits_latest_month_change"]
            if mvisits_change is not None:
                mvisits_change=round(mvisits_change*100, 2)

		
            if desc==None:
                desc=about
            
            rows2.append({'Company ID':semrush, 'Company Name': name, 'Name from URL': extract_company_name(company_url), 'Description': desc, 'Company Size': size, 'City': city, 
                            'State': state, 'Country': country, 'Business Category': cat, 
                            'Founded at (Date)': foundon, 'IPO status':ipo ,'E-mail': email, 'Phone:':phone,
                            'Linkedin': lin, 'Facebook': fb, 'Twitter': twt,
                            'Funding Rounds': fr, "Funding Total (USD)": ft_usd,
                            'Acquisitions': aqu, 'Investments': inve, 'Lead Investments': lis,
                            'Investors':invs, 'Lead Investors': inve2,
                            'Total Products Active': tpa, "Active Tech Count": atc,
                            'Monthly Visits': mvisits, "Monthly Visits Change(%)": mvisits_change,
                            'Similar Companies': similar})

        except Exception as e:
            print(e)
            errors.append({"msg":e, "urls":url})


def _reduce_organization_dataset(data: Dict) -> Dict:
    """
    Reduce the organization dataset to include only the desired fields.

    Args:
        data (Dict): The organization dataset to reduce.

    Returns:
        Dict: A dictionary containing the reduced organization data.
    """

    return jmespath.search(
        """{
        id: properties.identifier.permalink,
        name: properties.title,
        logo: properties.identifier.image_id,
        description: cards.overview_description.description,
        city: cards.company_about_fields2.location_identifiers[0].value,
        state: cards.company_about_fields2.location_identifiers[1].value,
        about: cards.about_short_description.short_description,
        country: cards.company_about_fields2.location_identifiers[2].value,
        size: cards.company_about_fields2.num_employees_enum,
        linkedin: cards.social_fields.linkedin.value,
        facebook: cards.social_fields.facebook.value,
        twitter: cards.social_fields.twitter.value,   
        email: cards.contact_fields.contact_email,
        phone: cards.contact_fields.phone_number,
        website: cards.company_about_fields2.website.value,
        ipo_status: cards.company_about_fields2.ipo_status,
        rank_org_company: cards.company_about_fields2.rank_org_company,
        semrush_global_rank: cards.semrush_summary.semrush_global_rank,
        semrush_visits_latest_month: cards.semrush_summary.semrush_visits_latest_month,
        semrush_visits_latest_month_change: cards.semrush_rank_headline.semrush_visits_mom_pct,
        semrush_id: cards.semrush_summary.identifier.permalink,
        semrush: cards.semrush_summary.identifier.uuid,       
        categories: cards.overview_fields_extended.categories[].value,
        legal_name: cards.overview_fields_extended.legal_name,
        operating_status: cards.overview_fields_extended.operating_status,
        last_funding_type: cards.overview_fields_extended.last_funding_type,
        founded_on: cards.overview_fields_extended.founded_on.value,
        advisors: cards.advisors_headline.num_current_advisor_positions,
        similar: cards.org_similarity_org_list[].identifier.value,
        funding_rounds: cards.funding_rounds_summary.num_funding_rounds,
        funding_usd: cards.funding_rounds_summary.funding_total.value_usd,
        acquisitions :  cards.acquisitions_list[].acquiree_identifier.value,
        investments: cards.investments_list[].organization_identifier.value,
        lead_investments: cards.investments_headline.num_lead_investments,
        investors: cards.investors_list[].investor_identifier.value,
        lead_investors: cards.investors_headline.num_lead_investors,
        total_products_active: cards.technology_highlights.siftery_num_products,
        active_tech_count: cards.technology_highlights.builtwith_num_technologies_used,
        location_groups: cards.overview_fields_extended.location_group_identifiers[].value,
        event_appearances: cards.event_appearances_headline.num_event_appearances
    }""",
        data,
    )


def _reduce_employee_dataset(data: Dict) -> List[Dict]:
    """
    Reduce the employee dataset to include only the desired fields.

    Args:
        data (Dict): The employee dataset to reduce.

    Returns:
        List[Dict]: A list of dictionaries containing the reduced employee data.
    """
    parsed = []
    for person in data["entities"]:
        parsed.append(
            jmespath.search(
                """{
                name: properties.name,    
                linkedin: properties.linkedin,
                job_levels: properties.job_levels,
                job_departments: properties.job_departments
                job_title: related_entities."contact_item.has_contact_item.forward".entities[0].properties.job_title
            }""",
                person,
            )
        )
    return parsed


def _reduce_person_dataset(dataset: dict) -> Dict:
    """
    Reduce the person dataset to include only the desired fields.

    Args:
        dataset (Dict): The person dataset to reduce.

    Returns:
        Dict: A dictionary containing the reduced person data.
    """
    parsed = jmespath.search(
        """{
        name: properties.identifier.value,
        title: properties.title,
        description: properties.short_description,
        type: properties.layout_id,
        gender: cards.overview_fields.gender,
        location_groups: cards.overview_fields.location_group_identifiers[].value,
        location: cards.overview_fields.location_identifiers[].value,
        current_jobs: cards.jobs_summary.num_current_jobs,
        past_jobs: cards.jobs_summary.num_past_jobs,
        education: cards.education_image_list[].school
        investing_overview: cards.investor_overview_headline,
        linkedin: cards.overview_fields2.linkedin.value,
        twitter: cards.overview_fields2.twitter.value,
        facebook: cards.overview_fields2.facebook.value,
        current_advisor_jobs: cards.investor_overview_headline.num_current_advisor_jobs,
        founded_orgs: cards.investor_overview_headline.num_founded_organizations,
        portfolio_orgs: cards.investor_overview_headline.num_portfolio_organizations,
        rank_principal_investor: cards.investor_overview_headline.rank_principal_investor
    }""",
        dataset,
    )
    return parsed



async def _scrape_sitemap_index(session: httpx.AsyncClient) -> List[str]:
    """
    Scrape Crunchbase Sitemap index for all sitemap URLs.

    Args:
        session (httpx.AsyncClient): The HTTP client session.

    Returns:
        List[str]: A list of sitemap URLs.
    """
    log.info("scraping sitemap index for sitemap urls")
    response = await session.get("https://www.crunchbase.com/www-sitemaps/sitemap-index.xml")
    sel = Selector(text=response.text)
    urls = sel.xpath("//sitemap/loc/text()").getall()
    log.info(f"found {len(urls)} sitemaps")
    return urls


def parse_sitemap(response) -> Iterator[Tuple[str, datetime]]:
    """
    Parse sitemap for location URLs and their last modification times.

    Args:
        response (httpx.Response): The HTTP response containing the sitemap.

    Returns:
        Iterator[Tuple[str, datetime]]: An iterator of tuples containing the URL and its last modification time.
    """
    sel = Selector(text=gzip.decompress(response.content).decode())
    urls = sel.xpath("//url")
    log.info(f"found {len(urls)} in sitemap {response.url}")
    for url_node in urls:
        url = url_node.xpath("loc/text()").get()
        last_modified = datetime.fromisoformat(url_node.xpath("lastmod/text()").get().strip("Z"))
        yield url, last_modified


async def discover_target(
    target: Literal["organizations", "people"], session: httpx.AsyncClient, min_last_modified=None
):
    """
    Discover URLs from a specific sitemap type.

    Args:
        target (Literal["organizations", "people"]): The target sitemap type to discover.
        session (httpx.AsyncClient): The HTTP client session.
        min_last_modified (datetime, optional): The minimum last modification time to filter the URLs.

    Yields:
        str: The discovered URL.
    """
    sitemap_urls = await _scrape_sitemap_index(session)
    urls = [url for url in sitemap_urls if target in url]
    log.info(f"found {len(urls)} matching sitemap urls (from total of {len(sitemap_urls)})")
    for url in urls:
        log.info(f"scraping sitemap: {url}")
        response = await session.get(url)
        for url, mod_time in parse_sitemap(response):
            if min_last_modified and mod_time < min_last_modified:
                continue  # skip
            yield url
            
def read_csv_with_fallback(file_path, columns):
    """
    Read a CSV file into a DataFrame with a fallback to an empty DataFrame if the file is not found.

    Args:
        file_path (str): The path to the CSV file.
        columns (list): List of column names to use for the fallback DataFrame if the file is not found.

    Returns:
        pandas.DataFrame: The DataFrame read from the CSV file, or an empty DataFrame with the specified columns if the file is not found.
    """
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError:
        log.info(f"No previous database found at {file_path}")
        return pd.DataFrame(columns=columns)
    
def convert_lists_to_strings(df):
    """
    Convert all list-type columns in the DataFrame to strings.

    Args:
        df (pandas.DataFrame): The DataFrame to convert.

    Returns:
        pandas.DataFrame: The DataFrame with list-type columns converted to strings.
    """
    for col in df.columns:
        if df[col].apply(type).eq(list).any():
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)
    return df



def update_and_save_db(new_data, db_path, columns):
    """
    Update an existing database with new data and save the result to a CSV file.

    Args:
        new_data (pandas.DataFrame): The new data to be added to the database.
        db_path (str): The path to the existing database CSV file.
        columns (list): List of column names to use if the database file is not found.
        subset (list, optional): List of column names to consider when identifying duplicates.

    Returns:
        pandas.DataFrame: The updated database.
    """
    previous_db = read_csv_with_fallback(db_path, columns)

    # Convert list-type columns to strings
    new_data = convert_lists_to_strings(new_data)
    previous_db = convert_lists_to_strings(previous_db)
    
    # Concatenate only if new_data is not empty
    if not new_data.empty:
        # Concatenate new data with previous database
        updated_db = pd.concat([previous_db, new_data], ignore_index=True)
        
        # Remove duplicate rows, considering only a subset of columns if provided
        updated_db.drop_duplicates(inplace=True)
    else:
        updated_db = previous_db

    updated_db.to_csv(db_path, index=False)
    return updated_db

async def run():
    """
    Main function to run the scraping tasks and save the results to CSV files.
    """
    
    BASE_CONFIG["cache"] = True

    # Read the file that contains the URLs to scrape
    matched_companies_df = pd.read_csv(f'./companies_matching/{datetime.now().year}-{datetime.now().month}-matched_companies_url.csv')
    urls = list(matched_companies_df['Company_name_url'])
    print(f'Number of companies to scrape: {len(urls)}')
    log.info(f'Number of companies to scrape: {len(urls)}')

    # Loop through the URLs defined above
    tasks = [scrape_company(url) for url in urls]
    for task in asyncio.as_completed(tasks):
        await task

    # Save People data to the database
    df = pd.DataFrame(rows)
    with open('log.txt', 'a') as log_w:
        log_w.write(f'Number of people fetched from Crunchbase: {df.shape[0]}\tDate: {str(date.today())}\n')
    new_people_db = update_and_save_db(df, './companies_information/people_data.csv', df.columns)
    log.info(f'Number of companies with contact information: {df["Company Name"].nunique()}')

    # Save Company data
    df2 = pd.DataFrame(rows2)
    new_companies_db = update_and_save_db(df2, './companies_information/companies_data.csv', df2.columns)
    # new_companies_db.to_csv('./companies_information/companies_data.csv', index=False)

    # Save the URLs that don't have People data
    df3 = pd.DataFrame(errors)
    new_errors_db = update_and_save_db(df3, './companies_information/errors.csv', df3.columns)
    # new_errors_db.to_csv('./companies_information/errors.csv', index=False)
    
    # Save only the contact information from people data to be used for dashboard and add all comapnies from job data
    df4 = new_people_db[['Company Name', 'Name from URL', 'People Name', 'Linkedin Profile', 'Job Title']].copy()
        # Bring job_data file to get all the companies
    jobs_detail_df = pd.read_csv('./consolidated_data/outputs.csv')
        # Step 1: Extract unique companies from job_data
    unique_companies_df = jobs_detail_df[['name']].drop_duplicates()
        # Step 2: Merge unique companies from job_data with matched_companies to match name fom indeed
    comp = pd.merge(unique_companies_df, matched_companies_df, left_on='name', right_on='indeed_name', how='left')
        # Step 3: Merge that last one with contact_data
    df4 = pd.merge(comp, df4, left_on='Crunchbase_name',right_on='Name from URL', how='left')
    df4=df4[['name','Company Name','Name from URL','People Name', 'Linkedin Profile', 'Job Title']]
        # Save to csv the dataframe
    df4.to_csv('./consolidated_data/contact.csv', index=False)


if __name__ == "__main__":
    asyncio.run(run())