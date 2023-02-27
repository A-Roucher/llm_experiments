import json
import requests
from tqdm import tqdm
import textdistance as td
import psycopg2
import json


def id_for_page(page):
    """Uses the wikipedia api to find the wikidata id for a page"""
    api = "https://en.wikipedia.org/w/api.php"
    query = f"?action=query&prop=pageprops&titles={page}&format=json"

    response = json.loads(requests.get(api + query).content)
    try:
        # Assume we got 1 page result and it is correct.
        page_info = list(response["query"]["pages"].values())[0]
        return page_info["pageprops"]["wikibase_item"]
    except:
        return None


def get_claim_as_time(claims, claim_id):
    """Helper function to work with data returned from wikidata api"""
    try:
        claim = claims[claim_id][0]["mainsnak"]["datavalue"]
        assert claim["type"] == "time", "Expecting time data type"
        return claim
    except:
        return None


def lifespan_for_id(wikidata_id):
    """Uses the wikidata API to retrieve wikidata for the given id."""
    data_url = "https://www.wikidata.org/wiki/Special:EntityData/%s.json"
    page = json.loads(requests.get(data_url % wikidata_id).content)
    claims = list(page["entities"].values())[0]["claims"]
    # P569 (birth) and P570 (death) ... not everyone has died yet.
    time = get_claim_as_time(claims, "P569")
    if time is not None:
        return time["value"]["time"]
    return "2000"


def confirm_birth_year_wikidata(author):
    page_id = id_for_page(author)
    if page_id is not None:
        birth = lifespan_for_id(page_id)
        birth_year = int(birth[0] + birth[1:].split("-")[0])
        if birth_year < 1940:
            return True
    return False


def deambiguate_authors(authors_list):  # not finished
    authors = {}
    for author in tqdm(authors_list):
        if author not in authors:
            for dict_author in authors.keys():
                if td.jaccard.normalized_similarity(dict_author, author) > 0.85:
                    print(author, dict_author)
            authors[author] = True
    return authors


def return_author_results(author):
    is_old_author = confirm_birth_year_wikidata(author)
    if not is_old_author:
        return None
    summary = requests.get(
        f'https://en.wikipedia.org/api/rest_v1/page/summary/{author.replace(" ", "_")}',
        timeout=2,
    ).json()
    print(summary)
    if "description" not in summary:
        return None
    name = summary
    name = summary["title"]
    description = summary["description"]
    thumbnail_url = None
    if "thumbnail" in summary:
        thumbnail_url = summary["thumbnail"]["source"]
    extract_html = summary["extract_html"]
    return (name, description, extract_html, thumbnail_url)


def open_sql_connection():
    with open("credentials.json", "rb") as file:
        password_railway = json.load(file)["password_railway"]
    conn = psycopg2.connect(
        database="railway",
        user="postgres",
        password=password_railway,
        host="containers-us-west-127.railway.app",
        port="5800",
    )
    conn.autocommit = True
    return conn
