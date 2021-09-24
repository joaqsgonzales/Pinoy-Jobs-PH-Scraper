import pandas as pd
import numpy as np
import requests
import re
import time
import random
from bs4 import BeautifulSoup
from tqdm.notebook import tqdm, trange


#### FRONT PAGE SCRAPER
#### 10 seconds to scrape data from 1 page

def pinoy_jobs_scraper(user_agent_list):
    """Return a dataframe containing info about job postings on the website.
    
    Parameters
    ----------
    user_agent_list : list
        A list of user agents, to avoid the scraper getting blocked when
        extracting a huge volume of data.
        
    Returns
    -------
    job_df : pandas.DataFrame
        A pandas dataframe containing the date when the job was posted,
        job title, the company offering the job, location of the company,
        along with the job description URLs.
    """

    # define last page of the website being scraped
    page_selector = 'div:nth-child(2) > div > div > ul > li:nth-child(5) > a'
    last_page_request = requests.get(f'https://pinoyjobs.ph/jobs/')
    last_page_soup = BeautifulSoup(last_page_request.text)
    last_page = int(last_page_soup.select(page_selector)[0].text)

    job_df = pd.DataFrame()
    page_range = tqdm(range(1566, last_page+1))
    
    for page in page_range: # scrape all pages
        # prettify progress bar
        page_range.set_description(f'Working on page"{page}"')
        
        # to randomize user agents for every data scraped
        user_agent = random.choice(user_agent_list)
        headers = {'User-Agent': user_agent}

        # scrape each page
        jobs = requests.get(f'https://pinoyjobs.ph/jobs/page/{page}/',
                            headers=headers)
        jobs_soup = BeautifulSoup(jobs.text)
        new_data = pd.DataFrame()

        # get date when job was posted
        date = jobs_soup.select('div > a > div > span')
        dates = pd.DataFrame(date, columns=['date_posted'])

        # get job title
        job_title = jobs_soup.select('div a div h2')
        job_titles = pd.DataFrame(job_title, columns=['job_title'])

        # get name of company offering job 
        company = jobs_soup.select('div > a > div > ul > li:nth-child(1)')
        company_names = pd.DataFrame(company, columns=['company_name'])

        # get location of company/job
        location = jobs_soup.select('div > a > div > ul > li:nth-child(3)')
        locations = pd.DataFrame(location, columns=['location'])

        # get URLs to access job descriptions
        urls_jobs = []
        for i in range(1, 31):
            # exception handling to break when the last job posting is reached
            try:
                url = f'div:nth-child(2) div div div:nth-child({i}) div a'
                url_job = jobs_soup.select(url)
                urls_jobs.append(url_job[0].attrs['href'])
            except:
                break

        urls = pd.DataFrame(urls_jobs, columns=['job_url'])

        # concatenate all information to a dataframe
        new_data = pd.concat([dates, job_titles, company_names, 
                              locations, urls], axis=1)
        job_df = pd.concat([job_df, new_data], ignore_index=True)
        
        # sleep to regulate scraping
        time.sleep(0.05)
    
    # save the scraped data to a csv
    job_df.to_csv('test_jobs.csv')
    
    return job_df


#### JOB LISTING URL SCRAPER
#### 10 seconds to scrape data from 10 URLs

def scrape_job_posting_urls(job_df, user_agent_list):
    """Return the jobs data with the job description and salary per posting.
    
    Parameters
    ----------
    job_df : pandas.DataFrame
        The returned dataframe after running the `pinoy_jobs_scraper()`
        function. This must contain the URL for each job posting in the
        website.
        
    user_agent_list : list
        A list of user agents, to avoid the scraper getting blocked when
        extracting a huge volume of data.
        
    Returns
    -------
    jobs_complete_df : pandas.DataFrame
        This dataframe contains all the information returned from the
        `pinoy_jobs_scraper()` function, along with the corresponding
        job description and salary range per job posting.
    """
    
    job_desc_list = []
    salary_list = []
    
    for job_url in tqdm(list(job_df['job_url'])):

        # to randomize user agents for every data scraped
        user_agent = random.choice(user_agent_list)
        headers = {'User-Agent': user_agent}
        details = requests.get(job_url, headers=headers)
        details_soup = BeautifulSoup(details.text)

        # job description info
        desc_selector = 'div > div.contents > div:nth-child(1) > div'
        try:
            job = details_soup.select(desc_selector)
            job_desc = " ".join([text for text in
                                 re.split('<[^>]*>', str(job[0]))
                                 if text != ''])
            job_desc = job_desc.replace('&amp', '')
            job_desc = job_desc.replace('\xa0', '')
            job_desc = job_desc.replace('\ufeff', '')
            job_desc_list.append(job_desc)
        except:
            job_desc_list.append(np.nan)

        # salary info
        salary_selector = 'p > i:nth-child(2)'
        try:
            salary = details_soup.select(salary_selector)
            salary_list.append(salary[0].text)
        except:
            salary_list.append(np.nan)

        # sleep to regulate scraping
        time.sleep(0.05)

    # merge dataset with first scraped data
    job_details = pd.DataFrame({'job_description': job_desc_list, 
                                'salary': salary_list})
    jobs_complete_df = pd.merge(job_df, job_details,
                                left_index=True, right_index=True)
    jobs_complete_df.to_csv('test_job_listings.csv')
    return jobs_complete_df

#### SCRAPER RUN

user_agents = [
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15'
'(KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0)'
'Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5)' 
'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0)'
'Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
'(KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0)' 
'Gecko/20100101 Firefox/15.0.1'
]

job_listings_df = pinoy_jobs_scraper(user_agents)
job_complete_df = scrape_job_posting_urls(job_listings_df, user_agents)

