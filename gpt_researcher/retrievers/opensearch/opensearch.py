import os
import requests
import json
import random

class ElasticSearch:
    """
    Elasticsearch / Opensearch API Retriever
    """
    def __init__(self, query, index, from_val=0, size=10, headers=None):
        """
        Initializes the ElasticSearch object.

        Args:
            query (str): The search query.
            index (str): The Elasticsearch index to search.
            from_val (int, optional): The offset for pagination (default: 0).
            size (int, optional): Maximum number of results to retrieve (default: 10).
            headers (dict, optional): Optional headers for the API request.
        """
        self.query = query
        self.index = index
        self.from_val = from_val
        self.size = size
        self.headers = headers or {"Content-Type": "application/json"}
        self.api_url = "https://localhost:9200"   # Get the list of Elasticsearch API URLs
        self.username = "admin"    # Fallback to environment variable if not provided
        self.password = "d72DM|h*v7xN"    # Fallback to environment variable if not provided
        # Seçilen URL, load balancing amacıyla ortam değişkenlerinden rastgele seçilebilir
        #self.api_url = random.choice(self.api_urls)

    def get_api_urls(self):
        """
        Gets the list of Elasticsearch API URLs.

        Returns:
            list: The list of Elasticsearch API URLs.
        """
        try:
            api_urls = os.environ["ELASTICSEARCH_API_URL"]
        except KeyError:
            raise Exception("Elasticsearch API URLs not found. Please set the ELASTICSEARCH_API_URLS environment variable.")
        return api_urls

    def get_username(self):
        """
        Gets the Elasticsearch username.

        Returns:
            str: The Elasticsearch username.
        """
        try:
            username = os.environ["ELASTICSEARCH_USERNAME"]
        except KeyError:
            raise Exception("Elasticsearch username not found. Please set the ELASTICSEARCH_USERNAME environment variable.")
        return username

    def get_password(self):
        """
        Gets the Elasticsearch password.

        Returns:
            str: The Elasticsearch password.
        """
        try:
            password = os.environ["ELASTICSEARCH_PASSWORD"]
        except KeyError:
            raise Exception("Elasticsearch password not found. Please set the ELASTICSEARCH_PASSWORD environment variable.")
        return password

    def search(self, from_val=None, size=None, sort=None,fields=None):
        """
        Searches the query in Elasticsearch.

        Implements an API request similar to:
          curl -X GET --header 'Accept: application/json' 
            'https://<elasticsearch_url>/<index>/_search?q=11&from=0&size=10&sort=1'

        Args:
            from_val (int, optional): The offset for pagination (default: self.from_val).
            size (int, optional): Maximum number of results to retrieve (default: self.size).
            sort (str, optional): Sorting parameter.

        Returns:
            dict: The search results.
        """
        from_val = from_val if from_val is not None else self.from_val
        size = size if size is not None else self.size

        url = f"{self.api_url}/{self.index}/_search"
        params = {
            "q": self.query,
            "from": from_val,
            "size": size,
            "sort": sort,
            "_source_includes": fields
        }

        # Temiz parametreler: None değerleri kaldırıyoruz.
        params = {k: v for k, v in params.items() if v is not None}

        # Sadece request seviyesinde ek header'lar ekleniyor.
        request_headers = {"Accept": "application/json"}
        extra_headers = self.headers.copy()
        extra_headers.update(request_headers)

        response = requests.get(url, auth=(self.username, self.password), params=params, headers=extra_headers,verify=False)
        response.raise_for_status()
        print(json.dumps(params, indent=2))
        return response.json()
    
if __name__ == "__main__":
    # Örnek: query=11, index "logs_index", from_val=0, size=1, sort parametresi: "timestamp:desc"
    search_instance = ElasticSearch(query="error", index="opensearch_dashboards_sample_data_logs", from_val=0, size=10)
    results = search_instance.search(sort="timestamp:desc",fields="tags,host")
    print(json.dumps(results, indent=2))