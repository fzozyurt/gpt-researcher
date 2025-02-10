# Tavily API Retriever

# libraries
import os
import requests
import json


class GraylogSearch:
    """
    Graylog API Retriever
    """
    def __init__(self, query, range=300, limit=10, fields=None, headers=None):
        """
        Initializes the GraylogSearch object.

        Args:
            query (str): The search query.
            range_val (int, optional): The time range for the search in seconds (default: 300).
            limit (int, optional): Maximum number of results to retrieve (default: 10).
            fields (str, optional): Fields to retrieve.
            headers (dict, optional): Optional headers for the API request.
        """
        self.headers=headers

        if headers is None:
            self.headers = {
            "Content-Type": "application/json",
            "X-Requested-By": "LLM Agent"
            }
        else:
            self.headers = headers
            if "Content-Type" not in self.headers:
                self.headers["Content-Type"] = "application/json"
            elif "X-Requested-By" not in self.headers:
                self.headers["X-Requested-By"] = "LLM Agent"

        self.api_urls = self.get_api_urls()  # Get the list of API URLs
        self.username = self.get_username()  # Use the passed username or fallback to environment variable
        self.password = self.get_password()  # Use the passed password or fallback to environment variable

        self.query = query
        self.range = range
        self.limit = limit
        self.fields = fields
    
    def get_api_urls(self):
        """
        Gets the list of Graylog API URLs
        Returns:
            list: The list of Graylog API URLs
        """
        try:
            api_urls = os.environ["GRAYLOG_API_URLS"].split(',')
        except KeyError:
            raise Exception("Graylog API URLs not found. Please set the GRAYLOG_API_URLS environment variable.")
        return api_urls

    def get_username(self):
        """
        Gets the Graylog username
        Returns:
            str: The Graylog username
        """
        try:
            username = os.environ["GRAYLOG_USERNAME"]
        except KeyError:
            raise Exception("Graylog username not found. Please set the GRAYLOG_USERNAME environment variable.")
        return username

    def get_password(self):
        """
        Gets the Graylog password
        Returns:
            str: The Graylog password
        """
        try:
            password = os.environ["GRAYLOG_PASSWORD"]
        except KeyError:
            raise Exception("Graylog password not found. Please set the GRAYLOG_PASSWORD environment variable.")
        return password
    def search(self, offset=None, filter_=None, sort=None, decorate="true"):
        """
        Searches the query in Graylog.

        Implements an API request similar to:
          curl -X GET --header 'Accept: application/json' --header 'X-Requested-By: Graylog API Browser'
          'https://<graylog_url>/api/search/universal/relative?query=11&range=tes&limit=1&offset=1&filter=1&fields=1&sort=1&decorate=true'

        Args:
            range_val (int, optional): The time range for the search in seconds (default: self.range_val).
            limit (int, optional): Maximum number of results to retrieve (default: self.limit).
            offset (int, optional): Offset for pagination.
            filter_ (str, optional): Filter parameter.
            fields (str, optional): Fields to retrieve (default: self.fields).
            sort (str, optional): Sorting parameter.
            decorate (str, optional): Decorate flag (default: "true").

        Returns:
            dict: The search results.
        """

        url = f"{self.api_url}/api/search/universal/relative"
        params = {
            "query": self.query,
            "range": self.range,
            "limit": self.limit,
            "offset": offset,
            "filter": filter_,
            "fields": self.fields,
            "sort": sort,
            "decorate": decorate,
        }
        
        # None olan parametreleri temizleyelim
        params = {k: v for k, v in params.items() if v is not None}

        response = requests.get(url, auth=(self.username, self.password), params=params,headers=self.headers)
        response.raise_for_status()
        return response.json()

if __name__ == "__main__":
    # Örnek: query=11, range "tes", limit=1, offset=1, filter=1, fields=1, sort=1
    search_instance = GraylogSearch(query="11", range="tes", limit=1, fields="1")
    results = search_instance.search(offset=1, filter_="1", sort="1")
    print(json.dumps(results, indent=2))