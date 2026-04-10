import pandas as pd
import json
import time
import os
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import random as r 

# adjust as needed
START_DATE = "2022-12-"
END_DATE = "2021-02-01"
KEY_WORD = "AI Environmental Impact"
COOKIE = "cookies.json"

QUERY = f"{KEY_WORD} since:{START_DATE} until:{END_DATE}"
SCROLL_PAUSE = r.randint(10,15)
MAX_SCROLLS = 100
PARTIAL_FILE = f"{START_DATE[:4]}_<{QUERY}>_tweets_partial.csv"
FINAL_FILE = f"{START_DATE[:4]}_'{QUERY}'_tweets.csv"

def save_stopped_date(date):
    """
    Save date after hitting twitter error
    """
    mode = "a" if os.path.exists("stopped_date.txt") else "w"
    with open("stopped_date.txt", mode, encoding="utf-8") as f:
        f.write(date + "\n")

def load_cookies(driver, path):
    """
    Load cookie for authentication
    """
    driver.get("https://x.com")  # Must be on the correct domain first
    time.sleep(2)  # wait for page to load

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    for name, value in raw.items():
        driver.add_cookie({
            "name": name,
            "value": value,
            "domain": ".x.com",
            "path": "/",
            "secure": True
        })
    driver.refresh()
    time.sleep(5)  # let cookies take effect

def save_scraped_data(tweets, filename=PARTIAL_FILE):
    """
    Save scraped data after scroll
    """
    df_new = pd.DataFrame(tweets)
    print("Columns in scraped DataFrame:", df_new.columns)  # debug

    if "tweet_id" in df_new.columns:
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            df_existing = pd.read_csv(filename)
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
            df_final.drop_duplicates(subset=["tweet_id"], inplace=True)
        else:
            df_final = df_new
        df_final.to_csv(filename, index=False)
        print(f"{len(df_final)} tweets saved to {filename}")
    else:
        # fallback: save anyway without dropping duplicates
        df_new.to_csv(filename, index=False)
        print(f"'tweet_id' missing. Saved {len(df_new)} tweets without de-duplication to {filename}")
      
def parse_metric(aria_label):
    """
    Parse metrics
    """
    match = re.search(r'([\d,.]+)([KMkm]?)', aria_label)
    if not match:
        return 0
    num_str, suffix = match.groups()
    num_str = num_str.replace(',', '')
    num = float(num_str)
    if suffix.upper() == "K":
        num *= 1_000
    elif suffix.upper() == "M":
        num *= 1_000_000
    return int(num)


def scrape_tweets():
    """
    Scrap Twitter data using Chrome Stealth Mode. Will save results to csv
    """
    # Initialize Chrome
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    service = Service()  # automatically finds chromedriver
    driver = webdriver.Chrome(service=service, options=options)

    # Apply stealth mode
    stealth(
        driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="MacIntel",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    
    # Load cookies
    load_cookies(driver, COOKIE)
    driver.refresh()
    time.sleep(5)  # let cookies take effect

    # check if logged in
    driver.get("https://x.com/home")
    time.sleep(5)

    # Go to search page
    driver.get(f"https://x.com/search?q={QUERY}&src=typed_query&f=live")
    time.sleep(3)

    # Load existing tweets if partial file exists to continue scraping
    if os.path.exists(PARTIAL_FILE) and os.path.getsize(PARTIAL_FILE) > 0:
        df_exisiting = pd.read_csv(PARTIAL_FILE)
        tweets = df_exisiting.to_dict("records")
        seen = {t["tweet_id"] for t in tweets if "tweet_id" in t}
        print(f"Resuming from {len(tweets)} tweets")
    else:
        tweets = []
        seen = set()

    scrolls = 0
    try: 
        while scrolls < MAX_SCROLLS:
            scrolls += 1
            new_tweets_this_scroll = 0

            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((By.XPATH, "//article")))

            elements = driver.find_elements(By.XPATH, "//article")

            if not elements:
                print("No tweets yet. Scrolling…")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(SCROLL_PAUSE)
                continue

            for el in elements:
                try:
                    # Detect Twitter error
                    if driver.find_elements(By.XPATH, "//*[contains(text(), 'Something went wrong')]"):
                        print("Twitter error. Stopping.")
                        save_scraped_data(tweets)
                        driver.quit()
                        return tweets

                    # Tweet ID
                    link_el = el.find_element(By.XPATH, ".//a[contains(@href,'/status/')]")
                    tweet_id = link_el.get_attribute("href").split("/")[-1]

                    if tweet_id in seen:
                        continue

                    # User info
                    try:
                        display_name = el.find_element(
                            By.XPATH, ".//div[@data-testid='User-Name']//span"
                        ).text
                    except:
                        display_name = None

                    try:
                        handle = link_el.get_attribute("href").split("/")[3]
                    except:
                        handle = None

                    # Date
                    date_el = el.find_element(By.TAG_NAME, "time")
                    date = date_el.get_attribute("datetime")

                    # Tweet text
                    text_block = el.find_elements(By.XPATH, ".//div[@data-testid='tweetText']")
                    tweet_text = " ".join([t.text for t in text_block])

                    # ---- Metrics ----
                    tweet_metrics = {
                        "replies": 0,
                        "reposts": 0,
                        "likes": 0,
                        "bookmarks": 0,
                        "views": 0
                    }

                    try:
                        stats_container = el.find_element(By.XPATH, ".//div[@role='group']")
                        for node in stats_container.find_elements(By.XPATH, ".//*"):
                            aria = node.get_attribute("aria-label")
                            if not aria:
                                continue

                            if "Reply" in aria:
                                tweet_metrics["replies"] = parse_metric(aria)
                            elif "Repost" in aria or "Retweet" in aria:
                                tweet_metrics["reposts"] = parse_metric(aria)
                            elif "Like" in aria:
                                tweet_metrics["likes"] = parse_metric(aria)
                            elif "Bookmark" in aria:
                                tweet_metrics["bookmarks"] = parse_metric(aria)
                            elif "View" in aria:
                                tweet_metrics["views"] = parse_metric(aria)
                    except:
                        print(f"Metrics missing for {tweet_id}")

                    # Base tweet data
                    tweet = {
                        "tweet_id": tweet_id,
                        "display_name": display_name,
                        "handle": handle,
                        "date": date,
                        "text": tweet_text,
                        **tweet_metrics
                        #"comments": []
                    }

                    # # ---- COMMENT SCRAPING ----
                    # # Only open the thread if replies > 0
                    # if tweet_metrics["replies"] > 0:
                    #     try:
                    #         driver.execute_script("arguments[0].scrollIntoView();", el)
                    #         driver.execute_script("arguments[0].click();", link_el)
                    #         time.sleep(5)

                    #         # Wait for reply tweets to load
                    #         thread_articles = driver.find_elements(By.XPATH, "//article")

                    #         for comment_el in thread_articles[1:]:  # Skip first (the main tweet)
                    #             try:
                    #                 comment_block = comment_el.find_elements(
                    #                     By.XPATH, ".//div[@data-testid='tweetText']"
                    #                 )
                    #                 c_text = " ".join([x.text for x in comment_block])
                    #                 if c_text.strip():
                    #                     tweet["comments"].append(c_text)
                    #             except:
                    #                 continue

                    #         driver.back()
                    #         time.sleep(1)

                    #     except Exception as e:
                    #         print(f"Error opening thread for {tweet_id}: {e}")

                    tweets.append(tweet)
                    seen.add(tweet_id)
                    new_tweets_this_scroll += 1

                    if len(tweets) % 10 == 0:
                        print(f"{len(tweets)} tweets collected.")
                except Exception as e:
                    print(f"Skipping tweet due to error: {e}")
                    continue
            # Scroll page
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE)

            if new_tweets_this_scroll == 0:
                print("No new tweets. Done.")
                break
    except TimeoutException:
        save_scraped_data(tweets)


    driver.quit()
    print(f"Stopped at {date}")
    save_scraped_data(tweets, filename=FINAL_FILE)
    return tweets

if __name__ == "__main__":
    data = scrape_tweets()
    print("Total tweets collected", len(data))
