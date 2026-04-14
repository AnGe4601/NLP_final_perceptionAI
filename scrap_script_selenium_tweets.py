import os
import json
import time
import random as r
import pandas as pd
from urllib.parse import quote
from datetime import datetime, timedelta
from googletrans import Translator
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth

translator = Translator()

# Configureation - Adjust as needed
COOKIE = "cookies.json"
SCROLL_PAUSE = r.randint(10,15)
MAX_SCROLLS = 100
TARGET_COUNT = 4000
SAVE_INTERVAL = 100
START_DATE = "2023-04-01"
END_DATE = "2026-04-01"

# Change target language for scraping
lang = "en"

# Reading from pre-defined csv file contained all keywords
keyword_list = pd.read_csv("search_keywords_list.csv")
keyword_list.rename(columns={"en": "en", "fr": "fr", "es": "es", "zh": "zh"})
QUERIES = [k.strip('') for k in keyword_list[lang].dropna().tolist()]


# File Helpers
def partial_file(lang):
    return f"tweets_{lang}_partial.csv"

def final_file(lang):
    return f"tweets_{lang}_final.csv"

def checkpoint_file(lang):
    """
    Checkpoint file helper
    """
    return f"checkpoint_{lang}.json"


def save_checkpoint(lang, keyword, week_start):
    """
    Save checkpoint of scrapping process
    """
    with open(checkpoint_file(lang), "w") as f:
        json.dump({
            "lang": lang, 
            "keyword": keyword,
            "week_start": week_start,
        }, f)
    print(f"Check point of {lang} | keywrod: {keyword} | week: {week_start}")

def load_checkpoint(lang):
    """
    Load checkpoint file to pick up process
    """
    path = checkpoint_file(lang)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None

def load_cookies(driver, path):
    """
    Load cookie for authentication
    """
    driver.get("https://x.com")  
    time.sleep(2)

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
    time.sleep(5)

def save_scraped_data(tweets, filename):
    """
    Save scraped data after scroll
    """
    if not tweets:
        return  
    df_new = pd.DataFrame(tweets)

    if "tweet_id" not in df_new.columns:
        print("Warning: tweet_id missing")
        df_new.to_csv(filename, index=False)
        return

    # Pick up from where was left off
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        df_existing = pd.read_csv(filename)
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
        df_final.drop_duplicates(subset=["tweet_id"], inplace=True)
    else:
        df_final = df_new
    df_final.to_csv(filename, index=False)
    print(f"[Saved] {len(df_final)} tweets as {filename}")

def save_tweets(tweet_id, search_lang, detected_lang, keyword, display_name, handle, date, text):
    """
    Helper 
    """
    return {
        "tweet_id":      tweet_id,
        "search_lang":   search_lang,    
        "detected_lang": detected_lang,  
        "keyword":       keyword,
        "display_name":  display_name,
        "handle":        handle,
        "date":          date,
        "text":          text
    }

def generate_timerange (start, end):
    """
    Generate and define timerange of data scrapping
    """
    current = datetime.strptime(start, "%Y-%m-%d")
    end_dt  = datetime.strptime(end,   "%Y-%m-%d")

    while current < end_dt:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day =1)
        else:
            next_month =  current.replace(month=current.month + 1, day=1)
        month_end = min(next_month, end_dt)
        yield current.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")
        current = month_end

def scrap_per_timerange(driver, lang, keyword, week_start, week_end, seen):
    """
    Scrap data at interval with pre-defined time range
    """
    print(week_start, week_end)
    
    # Go to search page
    encoded_query = quote(f"{keyword} since:{week_start} until:{week_end}")
    print(encoded_query)
    url = f"https://x.com/search?q={encoded_query}&src=typed_query&f=live&lang={lang}"

    driver.get(url)
    time.sleep(3)

    # Confirm at search page
    if "search" not in driver.current_url:
        print(f"[Warning] Failed to navigate to search page. Current URL: {driver.current_url}")
        return []

    new_tweets = []
    scrolls = 0
    try:
        while scrolls < MAX_SCROLLS:
            scrolls += 1
            soft_block_count = 0

            # Soft block detection
            if "Something went wrong" in driver.page_source:
                if soft_block_count <= 3:
                    print(f"[Soft block] {lang} | '{keyword}' | {week_start}. Waiting 60s...")
                    time.sleep(60)
                    driver.refresh()
                    time.sleep(5)
                    continue
                else:
                    save_checkpoint(lang, keyword, week_start)

            try:
                # Wait until some contents are being loaded
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//article"))
                )
            except TimeoutException:
                print(f"[Timeout] No tweets loaded for {week_start}.")
                break

            elements = driver.find_elements(By.XPATH, "//article")
            if not elements:
                print("No tweets found. Moving on.")
                break

            new_this_scroll = 0
            for el in elements:
                try:
                    link_el  = el.find_element(By.XPATH, ".//a[contains(@href,'/status/')]")
                    tweet_id = link_el.get_attribute("href").split("/")[-1]

                    if tweet_id in seen:
                        continue
                    try:
                        display_name = el.find_element(
                            By.XPATH, ".//div[@data-testid='User-Name']//span"
                        ).text
                    except Exception as e:
                        display_name = None

                    try:
                        handle = link_el.get_attribute("href").split("/")[3]
                    except Exception as e:
                        handle = None

                    date_el    = el.find_element(By.TAG_NAME, "time")
                    date       = date_el.get_attribute("datetime")
                    text_block = el.find_elements(By.XPATH, ".//div[@data-testid='tweetText']")
                    tweet_text = " ".join([t.text for t in text_block])

                    # Langage detection filter
                    try:
                        detected = translator.detect(tweet_text)
                        detected_lang = detected.lang
                    except:
                        detected_lang = "unknown"

                    if detected_lang in ["zh-cn", "zh-tw"]:
                        detected_lang = "zh"
                    elif detected_lang == "fr":
                        detected_lang = "fr"
                    elif detected_lang == "en":
                        detected_lang = "en"
                    elif detected_lang == "es":
                        detected_lang = "es"

                    tweet = save_tweets(tweet_id, lang, detected_lang, keyword, display_name, handle, date, tweet_text)

                    new_tweets.append(tweet)
                    seen.add(tweet_id)
                    new_this_scroll += 1

                except Exception as e:
                    print(f"Skipping tweet: {e}")
                    continue

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE)

            if new_this_scroll == 0:
                print("No new tweets this scroll. Moving to next week.")
                break
    except KeyboardInterrupt:
        print("\n[Interrupted] Saving partial progress...")
        return new_tweets  # return whatever was collected so far
    return new_tweets
    
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
    
    #for lang, keywords in QUERIES.items():
    print(f"\n{'='*50}\nStarting language: {lang}\n{'='*50}")

    # Load existing tweets for this language
    pfile = partial_file(lang)
    if os.path.exists(pfile) and os.path.getsize(pfile) > 0:
        df_existing = pd.read_csv(pfile)
        all_tweets  = df_existing.to_dict("records")
        seen = {t["tweet_id"] for t in all_tweets if "tweet_id" in t}
        print(f"[Resume] {len(all_tweets)} tweets already collected for {lang}")
    else:
        all_tweets = []
        seen = set()

    # Load checkpoint to know where we left off
    checkpoint   = load_checkpoint(lang)
    start_kw     = checkpoint["keyword"]   if checkpoint else None
    start_week   = checkpoint["week_start"] if checkpoint else None
    reached_start = (checkpoint is None)  # if no checkpoint, start from beginning
        
    # print(f"[DEBUG] lang: {lang}")
    # print(f"[DEBUG] keywords: {keywords}")
    # print(f"[DEBUG] all_tweets count: {len(all_tweets)}")
    # print(f"[DEBUG] TARGET_COUNT: {TARGET_COUNT}")
    # print(f"[DEBUG] checkpoint: {checkpoint}")
    # print(f"[DEBUG] reached_start: {reached_start}")


    for keyword in QUERIES:
        if len(all_tweets) >= TARGET_COUNT:
            print(f"[Done] Already have {len(all_tweets)} tweets for {lang}. Skipping.")
            break
        
        # Skip keywords before the checkpointed one
        if not reached_start and keyword != start_kw:
            print(f"[Skip keyword] '{keyword}'")
            continue
            
        for week_start, week_end in generate_timerange(START_DATE, END_DATE):
            if len(all_tweets) >= TARGET_COUNT:
                break

            # Skip months before the checkpointed month
            if not reached_start and week_start < start_week:
                print(f"[Skip month] {week_start}")
                continue
                
            reached_start = True  # ← set it once we're past the skipping phase

            print(f"[Scraping] {lang} | '{keyword}' | {week_start} → {week_end}")
            print(f"[DEBUG] About to call scrap_per_timerange for {lang} | '{keyword}' | {week_start} → {week_end}")

            new_tweets = scrap_per_timerange(driver, lang, keyword, week_start, week_end, seen)
            print(f"[DEBUG] scrap_per_timerange returned: {new_tweets}")
            if new_tweets:
                all_tweets.extend(new_tweets)

            save_scraped_data(all_tweets, pfile)
            save_checkpoint(lang, keyword, week_start)
            time.sleep(5)

    # Save final file for this language
    if all_tweets:
        save_scraped_data(all_tweets, final_file(lang))
        print(f"[Final] {lang}: {len(all_tweets)} tweets saved to {final_file(lang)}")

    driver.quit()
    print("\nDone Scraping")

if __name__ == "__main__":
    scrape_tweets()