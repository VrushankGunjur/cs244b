import requests
import time
import random
from collections import defaultdict
import sys

proxies = {
    "http": "http://localhost:5001",
}

websites = [
    "http://www.google.com",
    "http://en.wikipedia.org/wiki/Main_Page",
    "http://www.youtube.com",
    "http://www.reddit.com/",
    "http://www.tsinghua.edu.cn/en/",
    "http://www.nytimes.com/",
    "http://www.cnn.com/",
    "http://www.foxnews.com/",
    "http://www.stanford.edu/",
    "http://stackoverflow.com/",
]

def load_balance_test(num_iters):
    for _ in range(num_iters):
        for url in websites:
            try:
                r = requests.get(url, proxies = proxies)
                print('request made')
            except Exception as e:
                print(e)
        
if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        if test_name == '--load_balance':
            load_balance_test(int(sys.argv[2]))