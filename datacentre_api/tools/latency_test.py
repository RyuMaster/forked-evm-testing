import asyncio
import httpx
import time
import csv
import random
import os
import json
import urllib.parse
from collections import defaultdict
import itertools  # Import itertools for combinations

API_BASE_URL = 'http://127.0.0.1:8000'  # Replace with your actual API base URL

async def test_endpoint(client, endpoint, params):
    url = API_BASE_URL + endpoint
    try:
        start_time = time.perf_counter()
        response = await client.get(url, params=params)
        response_time = (time.perf_counter() - start_time) * 1000  # Convert to milliseconds
        response.raise_for_status()
        content_size = len(response.content)
        # Attempt to get number of items if response is JSON
        try:
            data = response.json()
            item_count = len(data.get('items', []))
            total_pages = data.get('total_pages', None)
        except Exception:
            item_count = None
            total_pages = None
        error_status = False
    except Exception as e:
        response_time = (time.perf_counter() - start_time) * 1000
        content_size = 0
        item_count = None
        total_pages = None
        error_status = True
    full_url = f"{url}?{urllib.parse.urlencode(params, doseq=True)}"
    return {
        'endpoint': endpoint,
        'full_url': full_url,
        'parameters': params,
        'response_time_ms': response_time,
        'content_size_bytes': content_size,
        'item_count': item_count,
        'total_pages': total_pages,
        'error_status': error_status,
    }

def analyze_results(results):
    print("\nGenerating report...")
    slow_requests = sorted(results, key=lambda x: x['response_time_ms'], reverse=True)[:10]
    error_requests = [r for r in results if r['error_status']]
    zero_result_requests = [r for r in results if r.get('item_count') == 0]
    total_requests = len(results)
    zero_result_count = len(zero_result_requests)
    error_count = len(error_requests)
    average_response_time = sum(r['response_time_ms'] for r in results) / total_requests

    endpoint_stats = defaultdict(lambda: {'total': 0, 'errors': 0, 'zeros': 0})
    problematic_filters = defaultdict(int)

    for r in results:
        endpoint = r['endpoint']
        endpoint_stats[endpoint]['total'] += 1
        if r['error_status']:
            endpoint_stats[endpoint]['errors'] += 1
        if r.get('item_count') == 0:
            endpoint_stats[endpoint]['zeros'] += 1
            params = r['parameters']
            for key in params.keys():
                if key in ['per_page', 'page', 'sort_by', 'sort_order']:
                    continue  # Skip common parameters
                problematic_filters[(endpoint, key)] += 1

    print("\n--- Report ---\n")
    print(f"Total requests: {total_requests}")
    print(f"Total errors: {error_count}")
    print(f"Total zero-item responses: {zero_result_count}")
    print(f"Average response time: {average_response_time:.2f} ms")

    for endpoint, stats in endpoint_stats.items():
        print(f"\nEndpoint: {endpoint}")
        print(f"  Total requests: {stats['total']}")
        print(f"  Errors: {stats['errors']}")
        print(f"  Zero-item responses: {stats['zeros']}")

    print("\nTop 10 slowest requests:")
    for r in slow_requests:
        print(f"Endpoint: {r['endpoint']}, Response time: {r['response_time_ms']:.2f} ms")
        print(f"  URL: {r['full_url']}\n")

    if error_requests:
        print("\nRequests resulted in errors (up to 10 examples):")
        for r in error_requests[:10]:
            print(f"Endpoint: {r['endpoint']}, Error, URL: {r['full_url']}")
    else:
        print("\nNo requests resulted in errors.")

    if zero_result_requests:
        print("\nRequests with zero items returned (up to 10 examples):")
        for r in zero_result_requests[:10]:
            print(f"Endpoint: {r['endpoint']}, Zero items, Params: {r['parameters']}")
    else:
        print("\nNo requests returned zero items.")

    if problematic_filters:
        print("\nProblematic filters causing zero results (Top 10):")
        sorted_filters = sorted(problematic_filters.items(), key=lambda x: x[1], reverse=True)[:10]
        for ((endpoint, filter_key), count) in sorted_filters:
            print(f"Endpoint: {endpoint}, Filter: {filter_key}, Occurrences: {count}")
    else:
        print("\nNo problematic filters identified.")

    print("\n--- End of Report ---\n")

async def main():
    results = []
    tests = []

    # Define per endpoint filters and parameters
    endpoints = [
        # /clubs endpoint
        {
            'name': 'clubs',
            'endpoint': '/clubs',
            'filters': {
                'club_id': [33, 40, 50],
                'country_id': ['ENG'],
                'owned': ['snailbrain', 'AndyG', 'vivaldi'],
                'balance_min': [500000000],
                'balance_max': [1000000000],
                'division_start_min': [2],
                'division_start_max': [5],
                'fans_start_min': [1000],
                'fans_start_max': [5000],
                'stadium_size_start_min': [10000],
                'stadium_size_start_max': [50000],
                'value_min': [100000000],
                'value_max': [1000000000],
                'rating_min': [60],
                'rating_max': [90],
                'manager_locked_min': [0],
                'manager_locked_max': [1],
                'transfers_in_min': [0],
                'transfers_in_max': [10],
            },
            'sort_by_options': ['club_id', 'last_price', 'volume_1_day', 'volume_7_day', 'balance', 'manager_name', 'country_id', 'value', 'rating'],
            'sort_order_options': ['asc', 'desc'],
            'pages': [1, 2],
        },
        # /clubs/detailed endpoint
        {
            'name': 'clubs_detailed',
            'endpoint': '/clubs/detailed',
            'filters': {
                'club_id': [33, 40, 50],
                'country_id': ['ENG'],
                'owned': ['snailbrain', 'AndyG', 'vivaldi'],
                'balance_min': [500000000],
                'balance_max': [1000000000],
                'division_start_min': [2],
                'division_start_max': [5],
                'fans_start_min': [1000],
                'fans_start_max': [5000],
                'stadium_size_start_min': [10000],
                'stadium_size_start_max': [50000],
                'value_min': [100000000],
                'value_max': [1000000000],
                'rating_min': [60],
                'rating_max': [90],
                'manager_locked_min': [0],
                'manager_locked_max': [1],
                'transfers_in_min': [0],
                'transfers_in_max': [10],
            },
            'sort_by_options': ['club_id', 'balance', 'division_start', 'stadium_size_start', 'value', 'rating', 'manager_locked', 'transfers_in', 'transfers_out'],
            'sort_order_options': ['asc', 'desc'],
            'pages': [1, 2],
        },
        # /players endpoint
        {
            'name': 'players',
            'endpoint': '/players',
            'filters': {
                'player_id': [154],
                'age_min': [20],
                'age_max': [60],
                'country_id': ['ESP'],
                'owned': ['snailbrain', 'AndyG', 'vivaldi'],
                'rating_min': [70],
                'rating_max': [90],
                'value_min': [5000000],
                'value_max': [200000000],
            },
            'sort_by_options': ['player_id', 'last_price', 'volume_1_day', 'volume_7_day', 'wages', 'rating', 'country_id', 'dob', 'value', 'club_id'],
            'sort_order_options': ['asc', 'desc'],
            'pages': [1, 2],
        },
        # /players/detailed endpoint
        {
            'name': 'players_detailed',
            'endpoint': '/players/detailed',
            'filters': {
                'player_id': [154],
                'age_min': [20],
                'age_max': [30],
                'country_id': ['ESP'],
                'owned': ['snailbrain', 'AndyG', 'vivaldi'],
                'rating_min': [70],
                'rating_max': [90],
                'value_min': [5000000],
                'value_max': [20000000],
            },
            'sort_by_options': ['player_id', 'wages', 'rating', 'value', 'club_id', 'fitness', 'morale', 'injured'],
            'sort_order_options': ['asc', 'desc'],
            'pages': [1, 2],
        },
        # /users endpoint
        {
            'name': 'users',
            'endpoint': '/users',
            'filters': {
                'name_prefix': ['sn', 'An'],
                'names': ['snailbrain', 'AndyG', 'vivaldi'],
            },
            'sort_by_options': ['name', 'balance', 'last_active', 'club_id'],
            'sort_order_options': ['asc', 'desc'],
            'pages': [1, 2],
        },
        # /users/detailed endpoint
        {
            'name': 'users_detailed',
            'endpoint': '/users/detailed',
            'filters': {
                'name_prefix': ['sn', 'An'],
                'names': ['snailbrain', 'AndyG', 'vivaldi'],
            },
            'sort_by_options': [
                'name', 'balance', 'last_active', 'club_id',
                'buy_volume_1_day', 'sell_volume_1_day', 'total_volume',
                'biggest_trade',
            ],
            'sort_order_options': ['asc', 'desc'],
            'pages': [1, 2],
        },
        # /share_trade_history endpoint
        {
            'name': 'share_trade_history',
            'endpoint': '/share_trade_history',
            'filters': [
                {'name': ['snailbrain', 'AndyG', 'vivaldi']},
                {'club_id': [33, 40, 50]},
                {'player_id': [154]},
            ],
            'sort_by_options': [],
            'sort_order_options': [],
            'pages': [1, 2],
        },
        # /trading_graph endpoint
        {
            'name': 'trading_graph',
            'endpoint': '/trading_graph',
            'filters': [
                {'club_id': [33, 40, 50], 'time_range': ['7d', '30d']},
                {'player_id': [154], 'time_range': ['30d', '1y']},
            ],
            'sort_by_options': [],
            'sort_order_options': [],
            'pages': [],
        },
        # /share_balances endpoint
        {
            'name': 'share_balances',
            'endpoint': '/share_balances',
            'filters': [
                {'name': ['snailbrain', 'AndyG', 'vivaldi']},
                {'club_id': [33, 40, 50]},
                {'player_id': [154]},
            ],
            'sort_by_options': ['name', 'share_type', 'share_id', 'num'],
            'sort_order_options': ['asc', 'desc'],
            'pages': [1, 2],
        },
    ]

    # Generate tests
    for ep in endpoints:
        endpoint = ep['endpoint']
        filters = ep.get('filters', {})
        sort_by_options = ep.get('sort_by_options', [])
        sort_order_options = ep.get('sort_order_options', ['asc', 'desc'])
        pages = ep.get('pages', [1])

        # Start with a basic test (no filters)
        tests.append((endpoint, {}))

        # Test pagination
        for page in pages:
            params = {'page': page, 'per_page': 50}
            tests.append((endpoint, params))

        # For endpoints where 'filters' is a list of dicts
        if isinstance(filters, list):
            for filter_dict in filters:
                # Each filter_dict may contain multiple keys
                keys = list(filter_dict.keys())
                values_list = [filter_dict[key] for key in keys]
                # Generate combinations of values
                for values in itertools.product(*values_list):
                    params = dict(zip(keys, values))
                    tests.append((endpoint, params))
        else:
            # For each filter, create tests
            for filter_name, values in filters.items():
                for value in values:
                    params = {filter_name: value}
                    tests.append((endpoint, params))

        # Test sorting options
        if sort_by_options:
            for sort_by in sort_by_options:
                for sort_order in sort_order_options:
                    params = {'sort_by': sort_by, 'sort_order': sort_order}
                    tests.append((endpoint, params))

        # Test combinations (unlimited)
        if isinstance(filters, dict):
            # Generate all combinations of filters, sort options, and pagination
            filter_items = list(filters.items())
            filter_names = [k for k, v in filter_items]
            filter_values_list = [v for k, v in filter_items]
            filter_combinations = list(itertools.product(*filter_values_list))
            for filter_values in filter_combinations:
                params = dict(zip(filter_names, filter_values))
                for sort_by in sort_by_options:
                    for sort_order in sort_order_options:
                        params_with_sort = params.copy()
                        params_with_sort.update({'sort_by': sort_by, 'sort_order': sort_order, 'page': 1, 'per_page': 50})
                        tests.append((endpoint, params_with_sort))

    # Randomize tests
    random.shuffle(tests)

    # Begin testing
    total_tests = len(tests)
    print(f"Total tests to run: {total_tests}")

    # Create results directory if not exists
    if not os.path.exists('results'):
        os.makedirs('results')

    # Prepare CSV file
    fieldnames = ['endpoint', 'full_url', 'parameters', 'response_time_ms', 'content_size_bytes', 'item_count', 'total_pages', 'error_status']
    csv_file_path = 'results/api_test_results.csv'
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    test_counter = 0
    async with httpx.AsyncClient(timeout=180) as client:
        for endpoint, params in tests:
            test_counter += 1
            print(f"Running test {test_counter}/{total_tests} - Endpoint: {endpoint}, Params: {params}")
            result = await test_endpoint(client, endpoint, params)
            results.append(result)
            # Write to CSV
            with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                # Convert parameters to a JSON-like string for better readability
                result_copy = result.copy()
                result_copy['parameters'] = json.dumps(result_copy['parameters'])
                writer.writerow(result_copy)
            await asyncio.sleep(0.05)  # Delay

    # Analyze results
    analyze_results(results)

    print("Testing completed.")

if __name__ == '__main__':
    asyncio.run(main())