import requests
import time
import sys
import random
from collections import defaultdict

proxies = {
    "http": "http://localhost:5001",
    "https": "http://localhost:5001"
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

errors = defaultdict(list)

def mean(l):
    return sum(l) / len(l)

def sendRequest(url, webCache=True):
    try:
        if webCache:
            r = requests.get(url, proxies=proxies)
        else:
            r = requests.get(url)
        return r.text
    except Exception as e:
        errors[url].append(e)
        return e

def timedRequest(url):
    start = time.perf_counter()
    response = sendRequest(url)
    end = time.perf_counter()
    return response, (end - start)

def testTypicalLatency(url):
    times = []
    for _ in range(20):
        start = time.perf_counter()
        response = sendRequest(url, webCache=False)
        end = time.perf_counter()
        times.append(end - start)
    return mean(times)


def stressTest(n=10000):
    typicalLatencies = defaultdict(float)
    for url in websites:
        print(f"testing typical latency for {url}")
        typicalLatencies[url] = testTypicalLatency(url)
        print(f"got {typicalLatencies[url]}")
        print(f"Errors during latency testing: {len(errors[url])}")

    times = defaultdict(list)
    for i in range(n):
        if i % 100 == 0:
            print(f"iteration {i}")
        url = random.choice(websites)
        response, elapsedTime = timedRequest(url)
        times[url].append(elapsedTime)
    
    for url, responseTimes in times.items():
        print(f"For {url}, typical response {typicalLatencies[url]}")
        print(f'    vs. avg cache time {mean(responseTimes)}, with {len(errors[url])} total errors')



if __name__ == "__main__":
    if len(sys.argv) > 1:
        stressTest(int(sys.argv[1]))
    else:
        stressTest()