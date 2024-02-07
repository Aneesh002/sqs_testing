# # import requests
# import json
# import logging
#
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger()
#
#
# def fetch_api(api_url):
#     """
#
#     :param api_url:
#     :return:
#     """
#     try:
#         response = requests.get(api_url)
#         response.raise_for_status()  # Raises an HTTPError if the response was an error
#         logger.info("API request successful")
#
#         messages = response.json()
#         messages_string = json.dumps(messages)
#
#         return messages_string
#     except requests.exceptions.RequestException as e:
#         logger.error(f"Error fetching API: {e}")
#         raise
