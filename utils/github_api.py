import time
from typing import Optional

import requests

from utils.token_manager import singleton_token_manager


def get_github_headers():
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "GitHubDataFetcher/1.0"
    }
    token = singleton_token_manager.get_token()
    if token:
        headers['Authorization'] = f"token {token}"
    return headers


def make_github_request(url: str) -> Optional[requests.Response]:
    for retry in range(3):
        try:
            response = requests.get(url, headers=get_github_headers())
            if response.status_code == 403:
                wait_time = 10
                time.sleep(wait_time)
                continue
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {str(e)}")
    return None
