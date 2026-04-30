import asyncio
import httpx
import time
import random
import logging
from collections import deque

logging.basicConfig(level=logging.ERROR)

API_BASE_URL = 'http://127.0.0.1:8000'

class Stats:
    def __init__(self):
        self.total_requests = 0
        self.total_errors = 0
        self.latencies = deque(maxlen=10000)  # To store enough latencies
        self.start_time = None  # We'll set this when the test starts

    def add_request(self, latency=None, error=False):
        self.total_requests += 1
        if error:
            self.total_errors += 1
        if latency is not None:
            self.latencies.append(latency)

    def get_stats(self):
        elapsed_time = time.time() - self.start_time if self.start_time else 0
        calls_per_sec = self.total_requests / elapsed_time if elapsed_time > 0 else 0
        avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
        return f"Total Calls: {self.total_requests}, Calls/sec: {calls_per_sec:.2f}, Avg Latency: {avg_latency:.3f}s, Errors: {self.total_errors}"

async def send_request(client: httpx.AsyncClient, endpoint: str, params: dict, stats: Stats):
    url = API_BASE_URL + endpoint
    try:
        start_time = time.time()
        response = await client.get(url, params=params)
        duration = time.time() - start_time
        response.raise_for_status()
        stats.add_request(latency=duration)
    except httpx.HTTPError as e:
        logging.error(f"HTTP error: {e} for URL: {url} with params: {params}")
        stats.add_request(error=True)
    except Exception as e:
        logging.error(f"Error: {e} for URL: {url} with params: {params}")
        stats.add_request(error=True)

async def worker(client: httpx.AsyncClient, request_queue: asyncio.Queue, stats: Stats):
    while True:
        item = await request_queue.get()
        if item is None:
            break
        endpoint, params = item
        await send_request(client, endpoint, params, stats)
        request_queue.task_done()

async def rate_limited_producer(request_queue, rate, duration, test_list):
    """
    Adds requests to the queue at a specified rate for a duration.
    """
    interval = 1.0 / rate
    end_time = time.time() + duration
    next_send_time = time.time()
    tests_iterator = iter(test_list)  # Initialize the iterator from test_list
    while time.time() < end_time:
        now = time.time()
        if now < next_send_time:
            await asyncio.sleep(next_send_time - now)
        try:
            test = next(tests_iterator)
        except StopIteration:
            # Reset the iterator by creating a new one from test_list
            tests_iterator = iter(test_list)
            test = next(tests_iterator)
        await request_queue.put(test)
        next_send_time += interval

async def print_stats(stats: Stats):
    prev_total_requests = stats.total_requests
    prev_time = time.time()
    while True:
        await asyncio.sleep(1)
        curr_total_requests = stats.total_requests
        curr_time = time.time()
        requests_since_last = curr_total_requests - prev_total_requests
        time_diff = curr_time - prev_time

        calls_per_sec = requests_since_last / time_diff if time_diff > 0 else 0

        avg_latency = sum(stats.latencies) / len(stats.latencies) if stats.latencies else 0
        print(f"Calls/sec: {calls_per_sec:.2f}, Avg Latency: {avg_latency:.3f}s, Errors: {stats.total_errors}", end='\r')

        prev_total_requests = curr_total_requests
        prev_time = curr_time

async def run_test_for_rate(client, rate, duration, tests):
    print(f"\nTesting at {rate} requests per second...")
    # Create a new Stats instance
    stats = Stats()

    # Ensure we have enough tests to cover the duration at the given rate
    total_requests_needed = int(rate * duration)
    # If we don't have enough tests, repeat them
    repeat_factor = total_requests_needed // len(tests) + 1
    test_list = tests * repeat_factor
    test_list = test_list[:total_requests_needed]
    random.shuffle(test_list)
    # Remove tests_iterator since we're now passing test_list directly
    # tests_iterator = iter(test_list)

    # Create the request queue
    request_queue = asyncio.Queue()

    # Start workers
    workers = []
    max_concurrent_requests = rate + 10  # Allow for some overhead

    for _ in range(max_concurrent_requests):
        worker_task = asyncio.create_task(worker(client, request_queue, stats))
        workers.append(worker_task)

    # Reset stats start_time
    stats.start_time = time.time()

    # Start the rate-limited producer, passing test_list instead of tests_iterator
    producer_task = asyncio.create_task(rate_limited_producer(request_queue, rate, duration, test_list))

    # Start the stats printer
    stats_task = asyncio.create_task(print_stats(stats))

    # Wait for duration
    await asyncio.sleep(duration)

    # After duration, cancel the producer
    producer_task.cancel()
    try:
        await producer_task
    except asyncio.CancelledError:
        pass

    # Wait for the queue to be empty
    await request_queue.join()

    # Stop workers
    for _ in workers:
        await request_queue.put(None)  # Signal to workers to exit
    await asyncio.gather(*workers)

    # Stop stats
    stats_task.cancel()
    try:
        await stats_task
    except asyncio.CancelledError:
        pass

    # Output the stats for this rate
    print(f"\nResults at {rate} requests per second:")
    print(stats.get_stats())
    return stats

async def main():
    results = []
    tests = []  # Initialize an empty list for tests

    # Common settings
    per_page_options = [50] # Enforce per_page value to control bulk request size
    pages = [1, 2, 10] # Include varied page numbers
    sort_orders = ['asc', 'desc'] # Ascending and descending order

    # Maximum tests per endpoint
    max_tests_per_endpoint = 250 # Cap the number of tests per endpoint

    # Function to generate tests per endpoint
    def generate_tests_for_endpoint(endpoint, param_sets):
        return [(endpoint, params) for params in param_sets]

    # Prepare tests for /clubs
    clubs_endpoint = '/clubs'
    clubs_sort_by_trading = ["last_price", "volume_1_day", "volume_7_day"]
    clubs_sort_by_clubs = ["club_id", "balance", "manager_name", "country_id"]
    clubs_sort_by = clubs_sort_by_trading + clubs_sort_by_clubs

    country_ids = ['ENG', 'ESP', 'GER', 'ITA', 'FRA'] # Sample country IDs

    clubs_param_sets = [
        {'sort_by': sort_by, 'sort_order': sort_order, 'per_page': per_page, 'page': page, 'country_id': country_id}
        for sort_by in clubs_sort_by
        for sort_order in sort_orders
        for per_page in per_page_options
        for page in pages
        for country_id in country_ids
    ]

    # Limit the number of clubs tests
    clubs_tests = generate_tests_for_endpoint(clubs_endpoint, clubs_param_sets)
    random.shuffle(clubs_tests)
    clubs_tests = clubs_tests[:max_tests_per_endpoint]
    tests.extend(clubs_tests)

    # Prepare tests for /players
    players_endpoint = '/players'
    players_detailed_endpoint = '/players/detailed'

    players_sort_by_trading = ['last_price', 'volume_1_day', 'volume_7_day']
    players_sort_by_players = ['player_id', 'wages', 'multi_position', 'rating', 'country_id', 'dob', 'value', 'club_id', 'agent_name']

    age_filters = [
        {'age_min': None, 'age_max': None}, {'age_min': 16, 'age_max': None}, {'age_min': 30, 'age_max': None},
        {'age_min': 40, 'age_max': None}, {'age_min': None, 'age_max': 18}, {'age_min': None, 'age_max': 24},
        {'age_min': None, 'age_max': 100}, {'age_min': 100, 'age_max': None},
    ]

    players_param_sets = [
        {k: v for k, v in {'sort_by': sort_by, 'sort_order': sort_order, 'per_page': per_page, 'page': page, **age_filter}.items() if v is not None}
        for sort_by in players_sort_by_trading + players_sort_by_players
        for sort_order in sort_orders
        for per_page in per_page_options
        for page in pages
        for age_filter in age_filters
        if not (age_filter['age_min'] and age_filter['age_max'])
    ]

    # Limit the number of players tests
    players_tests = generate_tests_for_endpoint(players_endpoint, players_param_sets)
    random.shuffle(players_tests)
    players_tests = players_tests[:max_tests_per_endpoint]
    tests.extend(players_tests)

    # Generate tests for /players/detailed
    players_detailed_param_sets = [
        {k: v for k, v in {'sort_by': sort_by, 'sort_order': sort_order, 'per_page': per_page, 'page': page, **age_filter}.items() if v is not None}
        for sort_by in players_sort_by_players
        for sort_order in sort_orders
        for per_page in per_page_options
        for page in pages
        for age_filter in age_filters
        if not (age_filter['age_min'] and age_filter['age_max'])
    ]

    # Limit the number of players detailed tests
    players_detailed_tests = generate_tests_for_endpoint(players_detailed_endpoint, players_detailed_param_sets)
    random.shuffle(players_detailed_tests)
    players_detailed_tests = players_detailed_tests[:max_tests_per_endpoint]
    tests.extend(players_detailed_tests)

    # Prepare tests for /users
    users_endpoint = '/users'
    users_detailed_endpoint = '/users/detailed'
    users_sort_by = ['name', 'balance', 'last_active_unix', 'club_id']
    users_detailed_sort_by = [
        'name', 'balance', 'last_active_unix', 'club_id', 'buy_volume_1_day', 'buy_volume_7_day', 'sell_volume_1_day',
        'sell_volume_7_day', 'buy_total_volume', 'sell_total_volume', 'total_volume', 'first_trade_date', 'tenth_trade_date',
        'hundredth_trade_date', 'thousandth_trade_date', 'biggest_trade'
    ]

    name_filters = [
        {'name_prefix': None, 'names': None}, {'name_prefix': 'sn', 'names': None}, {'name_prefix': 'zzz', 'names': None},
        {'name_prefix': None, 'names': ['nonexistent_user']}, {'name_prefix': None, 'names': ['snailbrain']},
        {'name_prefix': 'an', 'names': None}, {'name_prefix': 'en', 'names': None}, {'name_prefix': 'sn', 'names': None},
    ]

    users_param_sets = [
        {k: v for k, v in {'sort_by': sort_by, 'sort_order': sort_order, 'per_page': per_page, 'page': page, 'name_prefix': name_filter.get('name_prefix'), 'names': name_filter.get('names')}.items() if v}
        for sort_by in users_sort_by
        for sort_order in sort_orders
        for per_page in per_page_options
        for page in pages
        for name_filter in name_filters
        if not (name_filter.get('name_prefix') and name_filter.get('names'))
    ]

    # Limit the number of users tests
    users_tests = generate_tests_for_endpoint(users_endpoint, users_param_sets)
    random.shuffle(users_tests)
    users_tests = users_tests[:max_tests_per_endpoint]
    tests.extend(users_tests)

    # Generate tests for /users/detailed
    users_detailed_param_sets = [
        {k: v for k, v in {'sort_by': sort_by, 'sort_order': sort_order, 'per_page': per_page, 'page': page, 'name_prefix': name_filter.get('name_prefix'), 'names': name_filter.get('names')}.items() if v}
        for sort_by in users_detailed_sort_by
        for sort_order in sort_orders
        for per_page in per_page_options
        for page in pages
        for name_filter in name_filters
        if not (name_filter.get('name_prefix') and name_filter.get('names'))
    ]

    # Limit the number of users detailed tests
    users_detailed_tests = generate_tests_for_endpoint(users_detailed_endpoint, users_detailed_param_sets)
    random.shuffle(users_detailed_tests)
    users_detailed_tests = users_detailed_tests[:max_tests_per_endpoint]
    tests.extend(users_detailed_tests)

    # We now have a balanced number of tests per endpoint
    total_tests = len(tests)
    print(f"Total tests generated: {total_tests}")

    # Define the target rates and duration
    target_rates = [15, 30, 50, 75, 100, 150]
    duration_per_rate = 15  # seconds

    results = []

    async with httpx.AsyncClient(timeout=180) as client:
        for rate in target_rates:
            stats = await run_test_for_rate(client, rate, duration_per_rate, tests)
            results.append((rate, stats))

    print("\nAll tests completed.")

    # Optionally, print a summary
    for rate, stats in results:
        print(f"\nSummary for {rate} requests per second:")
        print(stats.get_stats())

if __name__ == '__main__':
    asyncio.run(main())