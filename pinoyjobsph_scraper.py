import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import re
from tqdm.notebook import tqdm, trange


#### FRONT PAGE SCRAPER
#### 10 seconds to scrape data from 1 page

def pinoy_jobs_scraper(last_page):
    """Return a dataframe containing info about job postings on the website.
    
    Parameters
    ----------
    last_page : int
        The page where the last listing in the website is located.
        
    Returns
    -------
    job_df : pandas.DataFrame
        A pandas dataframe containing the date when the job was posted,
        job title, the company offering the job, location of the company,
        along with the job description URLs.
    """

    job_df = pd.DataFrame()
    page_range = tqdm(range(1, last_page+1))

    for page in page_range: # scrape all pages
        # prettify progress bar
        page_range.set_description(f'Working on page"{page}"')

        # scrape each page
        jobs = requests.get(f'https://pinoyjobs.ph/jobs/page/{page}/')
        new_data = pd.DataFrame()
        jobs_soup = BeautifulSoup(jobs.text)

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
    
    # save the scraped data to a csv
    job_df.to_csv('test_jobs.csv')
    
    return job_df

# define last page
page_selector = 'div:nth-child(2) > div > div > ul > li:nth-child(5) > a'
last_page = requests.get(f'https://pinoyjobs.ph/jobs/')
last_page_soup = BeautifulSoup(last_page.text)
page_num = int(last_page_soup.select(page_selector)[0].text)

job_listings_df = pinoy_jobs_scraper(page_num)

