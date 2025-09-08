import ast
from datetime import datetime, timedelta
import json
import os
import re
import traceback

from flask import Flask, jsonify, request

import requests

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

DATE_FORMAT = '%d-%m-%Y %H:%M:%S'

DEBUG_LOG_LEVEL = 'DEBUG'
ERROR_LOG_LEVEL = 'ERROR'
INFO_LOG_LEVEL = 'INFO'
LOG_LEVEL = os.environ.get('LOG_LEVEL', ERROR_LOG_LEVEL)

SEPARATOR = '#@#'

VALID_SOURCES = {
    'ALL_SOURCE': 'all',
    'INVESTIDOR10_SOURCE': 'investidor10',
    'STOCKANALYSIS_SOURCE': 'stockanalysis'
}

VALID_INFOS = [
    'actuation',
    'assets_value',
    'avg_annual_dividends',
    'avg_price',
    'beta',
    'cagr_profit',
    'cagr_revenue',
    'debit',
    'dy',
    'ebit',
    'enterprise_value',
    'equity_price',
    'equity_value',
    'gross_margin',
    'initial_date',
    'latests_dividends',
    'link',
    'liquidity',
    'management_fee',
    'market_value',
    'max_52_weeks',
    'min_52_weeks',
    'name',
    'net_margin',
    'net_profit',
    'net_revenue',
    'payout',
    'pl',
    'price',
    'pvp',
    'roe',
    'roic',
    'sector',
    'total_issued_shares',
    'type',
    'variation_12m',
    'variation_30d'
]

app = Flask(__name__)
app.json.sort_keys = False

def log_error(message):
    if LOG_LEVEL == ERROR_LOG_LEVEL or LOG_LEVEL == INFO_LOG_LEVEL or LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {ERROR_LOG_LEVEL} - {message}')

def log_info(message):
    if LOG_LEVEL == INFO_LOG_LEVEL or LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {INFO_LOG_LEVEL} - {message}')

def log_debug(message):
    if LOG_LEVEL == DEBUG_LOG_LEVEL:
        print(f'{datetime.now().strftime(DATE_FORMAT)} - {DEBUG_LOG_LEVEL} - {message}')

def cache_exists():
    if os.path.exists(CACHE_FILE):
        return True

    log_info('No cache file found')
    return False

def upsert_cache(id, data):
    lines = []
    updated = False

    if cache_exists():
        with open(CACHE_FILE, 'r') as cache_file:
            lines = cache_file.readlines()

    with open(CACHE_FILE, 'w') as cache_file:
        for line in lines:
            if not line.startswith(id):
                cache_file.write(line)
                continue

            _, old_cached_date_as_text, old_data_as_text = line.strip().split(SEPARATOR)
            old_data = ast.literal_eval(old_data_as_text)

            combined_data = { **old_data, **data }
            updated_line = f'{id}{SEPARATOR}{old_cached_date_as_text}{SEPARATOR}{combined_data}\n'
            cache_file.write(updated_line)
            updated = True

        if not updated:
            new_line = f'{id}{SEPARATOR}{datetime.now().strftime(DATE_FORMAT)}{SEPARATOR}{data}\n'
            cache_file.write(new_line)
            log_info(f'New cache entry created for "{id}"')

    if updated:
        log_info(f'Cache updated for "{id}"')

def clear_cache(id):
    if not cache_exists():
        return

    log_debug('Cleaning cache')

    with open(CACHE_FILE, 'r') as cache_file:
        lines = cache_file.readlines()

    with open(CACHE_FILE, 'w') as cache_file:
        cache_file.writelines(line for line in lines if not line.startswith(id))

    log_info(f'Cache cleaning completed for "{id}"')

def read_cache(id):
    if not cache_exists():
        return None

    log_debug('Reading cache')

    clear_cache_control = False

    with open(CACHE_FILE, 'r') as cache_file:
        for line in cache_file:
            if not line.startswith(id):
                continue

            _, cached_date_as_text, data = line.strip().split(SEPARATOR)
            cached_date = datetime.strptime(cached_date_as_text, DATE_FORMAT)

            if datetime.now() - cached_date <= CACHE_EXPIRY:
                log_debug(f'Cache hit for "{id}" (Date: {cached_date_as_text})')
                return ast.literal_eval(data)

            log_debug(f'Cache expired for "{id}" (Date: {cached_date_as_text})')
            clear_cache_control = True
            break

    if clear_cache_control:
        clear_cache(id)

    log_info(f'No cache entry found for "{id}"')
    return None

def delete_cache():
    if not cache_exists():
        return

    log_debug('Deleting cache')

    os.remove(CACHE_FILE)

    log_info('Cache deletion completed')

def preprocess_cache(id, should_delete_all_cache, should_clear_cached_data, should_use_cache):
    if should_delete_all_cache:
        delete_cache()
    elif should_clear_cached_data:
        clear_cache(id)

    can_use_cache = should_use_cache and not (should_delete_all_cache or should_clear_cached_data)

    return can_use_cache

def get_substring(text, start_text, end_text, replace_by_paterns=[], should_remove_tags=False):
    start_index = text.find(start_text)
    new_text = text[start_index:]

    end_index = new_text[len(start_text):].find(end_text) + len(start_text)
    cutted_text = new_text[len(start_text):end_index]

    if not cutted_text:
        return None

    clean_text = cutted_text.replace('\n', '').replace('\t', '')

    no_tags_text = re.sub(r'<[^>]*>', '', clean_text) if should_remove_tags else clean_text

    final_text = no_tags_text
    for pattern in replace_by_paterns:
        final_text = final_text.replace(pattern, '')

    return final_text.strip()

def text_to_number(text, should_convert_thousand_decimal_separators=False, convert_percent_to_decimal=False):
    try:
        if not text:
            raise Exception()

        if not isinstance(text, str):
            return text

        text = text.strip()

        if not text.strip():
            raise Exception()

        if should_convert_thousand_decimal_separators:
            text = text.replace('.','').replace(',','.')
        else:
            text = text.replace(',','')

        if '%' in text:
            return float(text.replace('%', '').strip()) / (100 if convert_percent_to_decimal else 1)

        if 'R$' in text:
            text = text.replace('R$', '')
        elif 'US$' in text:
            text = text.replace('US$', '')
        elif '$' in text:
            text = text.replace('$', '')

        return float(text.strip())
    except:
        return 0

def multiply_by_unit(data):
    if not data:
        return None

    if 'K' in data:
        return text_to_number(data.replace('K', '')) * 1_000
    elif 'M' in data:
        return text_to_number(data.replace('Milhões', '').replace('M', '')) * 1_000_000
    elif 'B' in data:
        return text_to_number(data.replace('Bilhões', '').replace('B', '')) * 1_000_000_000
    elif 'T' in data:
        return text_to_number(data.replace('Trilhões', '').replace('T', '')) * 1_000_000_000_000

    return text_to_number(data)

def request_get(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    log_debug(f'Response from {url} : {response}')

    return response

def filter_remaining_infos(data, info_names, default_info_names=None):
    if not data:
        return info_names

    missing_info = [ info for info in info_names if info in data and data[info] is None ]

    return missing_info if missing_info else default_info_names

def combine_data(first_dict, second_dict, info_names):
    if first_dict and second_dict:
        combined_dict = {**first_dict, **second_dict}
        log_debug(f'Data from combined Frist and Second Dictionaries: {combined_dict}')
    elif first_dict:
        combined_dict = first_dict
        log_debug(f'Data from First Dictionary only: {combined_dict}')
    elif second_dict:
        combined_dict = second_dict
        log_debug(f'Data from Second Dictionary only: {combined_dict}')
    else:
        combined_dict = {}
        log_debug('No combined data')

    missing_combined_infos = filter_remaining_infos(combined_dict, info_names)
    log_debug(f'Missing info from Combined data: {missing_combined_infos}')
    return combined_dict, missing_combined_infos

def remove_type_from_name(text):
    return text.replace('REIT', '').replace('STOCK', '').replace('ETF', '').strip()

def get_leatests_dividends(dividends):
    get_leatest_dividend = lambda dividends, year: next((dividend['price'] for dividend in dividends if dividend['created_at'] == year), None)

    current_year = datetime.now().year

    value = get_leatest_dividend(dividends, current_year)

    return value if value else get_leatest_dividend(dividends, current_year -1)

def convert_investidor10_stock_or_reit_data(json_ticker_page, json_dividends_data, info_names):
    balance = max(json_ticker_page['balances'], key=lambda balance: datetime.strptime(balance['reference_date'], "%Y-%m-%dT%H:%M:%S.%fZ"))
    actual_price = max(json_ticker_page['quotations'], key=lambda quotation: datetime.strptime(quotation['date'], "%Y-%m-%dT%H:%M:%S.%fZ"))['price']

    ALL_INFO = {
        'actuation': lambda: json_ticker_page['industry']['name'],
        'assets_value': lambda: balance['total_assets'],
        'avg_annual_dividends': lambda: (sum(dividend['price'] for dividend in json_dividends_data) / len(json_dividends_data)) if json_dividends_data else None,
        'avg_price': lambda: None,
        'beta': lambda: None,
        'cagr_profit': lambda: balance['growth_net_profit_last_5_years'],
        'cagr_revenue': lambda: balance['growth_net_revenue_last_5_years'],
        'debit': lambda: text_to_number(balance['long_term_debt']),
        'dy': lambda: text_to_number(balance['dy']),
        'ebit': lambda: text_to_number(balance['ebit']),
        'enterprise_value': lambda: None,
        'equity_price': lambda: None,
        'equity_value': lambda: balance['total_equity'],
        'gross_margin': lambda: text_to_number(balance['gross_margin']),
        'initial_date': lambda: json_ticker_page['start_year_on_stock_exchange'],
        'latests_dividends': lambda: get_leatests_dividends(json_dividends_data),
        'link': lambda: None,
        'liquidity': lambda: balance['volume_avg'],
        'management_fee': lambda: None,
        'market_value': lambda: balance['market_cap'],
        'max_52_weeks': lambda: None,
        'min_52_weeks': lambda: None,
        'name': lambda: remove_type_from_name(json_ticker_page['company_name']),
        'net_margin': lambda: text_to_number(balance['net_margin']),
        'net_profit': lambda: balance['net_income'],
        'net_revenue': lambda: balance['revenue'],
        'payout': lambda: text_to_number(balance['api_info']['common_size_ratios']['dividend_payout_ratio']),
        'pl': lambda: text_to_number(balance['pl']),
        'price': lambda: actual_price,
        'pvp': lambda: text_to_number(balance['pvp']),
        'roe': lambda: text_to_number(balance['roe']),
        'roic': lambda: text_to_number(balance['roic']),
        'sector': lambda: json_ticker_page['industry']['sector']['name'],
        'total_issued_shares': lambda: balance['shares_outstanding'],
        'total_real_state': lambda: None,
        'type': lambda: json_ticker_page['type'],
        'vacancy': lambda: None,
        'variation_12m': lambda: balance['variation_year'],
        'variation_30d': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def get_stock_or_reit_from_investidor10(ticker, share_type, info_names):
    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': 'https://investidor10.com.br/reits/0/',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        }

        response = request_get(f'https://investidor10.com.br/{share_type}/{ticker}', headers)
        html_page =  response.text[15898:]

        json_data = get_substring(html_page, 'var mainTicker =', 'var ')[:-1]
        json_ticker_page  = json.loads(json_data)

        json_dividends_data = {}
        if 'latests_dividends' in info_names or 'avg_annual_dividends' in info_names:
            response = request_get(f'https://investidor10.com.br/api/stock/dividendos/chart/{json_ticker_page["id"]}/3650/ano', headers)
            json_dividends_data = response.json()

        converted_data = convert_investidor10_stock_or_reit_data(json_ticker_page, json_dividends_data, info_names)
        log_debug(f'Converted fresh Investidor 10 data: {converted_data}')
        return converted_data
    except Exception as error:
        log_error(f'Error fetching data from Investidor 10 for "{ticker}": {traceback.format_exc()}')
        return None

def convert_stockanalysis_stock_or_reit_data(ticker, share_type, initial_page, statistics_page, info_names):
    roa = text_to_number(get_substring(statistics_page, 'ROA)",value:"', '%'))
    net_profit = multiply_by_unit(get_substring(initial_page, 'netIncome:"', '",'))

    avg_annual_dividends = text_to_number(get_substring(statistics_page, 'Dividend Per Share",value:"$', '",'))

    ALL_INFO = {
        'actuation': lambda: get_substring(initial_page, 'Industry",v:"', '",'),
        'assets_value': lambda: net_profit / roa,
        'avg_annual_dividends': lambda: avg_annual_dividends,
        'avg_price': lambda: text_to_number(get_substring(statistics_page, '200-Day Moving Average",value:"', '",')),
        'beta': lambda: get_substring(statistics_page, 'Beta (5Y)",value:"', '",'),
        'cagr_profit': lambda: None,
        'cagr_revenue': lambda: None,
        'debit': lambda: multiply_by_unit(get_substring(statistics_page, 'Debt",value:"', '",')),
        'dy': lambda: text_to_number(get_substring(statistics_page, 'Dividend Yield",value:"', '%')),
        'ebit': lambda: multiply_by_unit(get_substring(statistics_page, 'EBIT",value:"', '",')),
        'enterprise_value': lambda: multiply_by_unit(get_substring(statistics_page, 'Enterprise Value",value:"', '",')),
        'equity_price': lambda: None,
        'equity_value': lambda: None,
        'gross_margin': lambda: multiply_by_unit(get_substring(statistics_page, 'Gross Margin",value:"', '%')),
        'initial_date': lambda: get_substring(initial_page, 'inception:"', '",'),
        'latests_dividends': lambda: avg_annual_dividends / 12,
        'link': lambda: f'https://stockanalysis.com/stocks/{ticker}/company/',
        #'link': lambda: get_substring(initial_page, 'Website",v:"', '",'),
        'liquidity': lambda: text_to_number(get_substring(statistics_page, 'Average Volume (20 Days)",value:"', '",')),
        #'liquidity': lambda: get_substring(initial_page, 'v:', '",'),
        'management_fee': lambda: None,
        'market_value': lambda: multiply_by_unit(get_substring(statistics_page, 'Market Cap",value:"', '",')),
        'max_52_weeks': lambda: get_substring(initial_page, 'h52:', ','),
        'min_52_weeks': lambda: get_substring(initial_page, 'l52:', ','),
        'name': lambda: get_substring(initial_page, 'nameFull:"', '",'),
        'net_margin': lambda: multiply_by_unit(get_substring(statistics_page, 'Operating Margin",value:"', '%')),
        'net_profit': lambda: net_profit,
        'net_revenue': lambda: multiply_by_unit(get_substring(initial_page, 'revenue:"', '",')),
        'payout': lambda: text_to_number(get_substring(statistics_page, 'Payout Ratio",value:"', '%')),
        'pl': lambda: get_substring(initial_page, 'peRatio:"', '",'),
        'price': lambda: get_substring(initial_page, 'cl:', ','),
        'pvp': lambda: None,
        'roe': lambda: text_to_number(get_substring(statistics_page, 'ROE)",value:"', '%')),
        'roe': lambda: text_to_number(get_substring(statistics_page, 'ROIC)",value:"', '%')),
        'sector': lambda: get_substring(initial_page, 'Sector",v:"', '",'),
        'total_issued_shares': lambda: multiply_by_unit(get_substring(initial_page, 'sharesOut:"', '",')),
        'total_real_state': lambda: None,
        'type': lambda: share_type[:-1].upper(),
        'vacancy': lambda: None,
        'variation_12m': lambda: text_to_number(get_substring(statistics_page, '52-Week Price Change",value:"', '%')),
        'variation_30d': lambda: None,
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def get_stock_or_reit_from_stockanalysis(ticker, share_type, info_names):
    try:
        headers = {
            'accept': '*/*',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'no-cache',
            'dnt': '1',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'referer': 'https://stockanalysis.com/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 OPR/118.0.0.0',
        }

        response =  request_get(f'https://stockanalysis.com/stocks/{ticker}', headers)
        initial_page = get_substring(response.text[5_000:], 'Promise.all([', 'news:')

        response =  request_get(f'https://stockanalysis.com/stocks/{ticker}/statistics', headers)
        statistics_page = get_substring(response.text[5_000:], 'Promise.all([', ';')

        converted_data = convert_stockanalysis_stock_or_reit_data(ticker, share_type, initial_page, statistics_page, info_names)
        log_debug(f'Converted fresh Stock Analysis data: {converted_data}')
        return converted_data
    except Exception as error:
        log_error(f'Error fetching data from Stock Analysis for "{ticker}": {traceback.format_exc()}')
        return None

def get_stock_or_reit_from_all_sources(ticker, share_type, info_names):
    data_stockanalysis = get_stock_or_reit_from_stockanalysis(ticker, share_type, info_names)
    log_info(f'Data from Stock Analysis: {data_stockanalysis}')

    missing_stockanalysis_infos = filter_remaining_infos(data_stockanalysis, info_names)
    log_debug(f'Missing info from Stock Analysis: {missing_stockanalysis_infos}')

    if data_stockanalysis and not missing_stockanalysis_infos:
        return data_stockanalysis

    data_investidor_10 = get_stock_or_reit_from_investidor10(ticker, share_type, missing_stockanalysis_infos or info_names)
    log_info(f'Data from Investidor 10: {data_investidor_10}')

    if not data_investidor_10:
        return data_stockanalysis

    return { **data_stockanalysis, **data_investidor_10 }

def get_stock_or_reit_from_sources(ticker, share_type, source, info_names):
    SOURCES = {
        VALID_SOURCES['STOCKANALYSIS_SOURCE']: get_stock_or_reit_from_stockanalysis,
        VALID_SOURCES['INVESTIDOR10_SOURCE']: get_stock_or_reit_from_investidor10
    }

    fetch_function = SOURCES.get(source, get_stock_or_reit_from_all_sources)
    return fetch_function(ticker, share_type, info_names)

def convert_investidor10_etf_data(html_page, json_dividends_data, info_names):
    patterns_to_remove = [
        '</div>',
        '<div>',
        '<div class="value">',
        '<div class="_card-body">',
        '</span>',
        '<span>',
        '<span class="value">'
    ]

    ALL_INFO = {
        'actuation': lambda: None,
        'assets_value': lambda: multiply_by_unit(get_substring(html_page, 'Capitalização</span>', '</span>', patterns_to_remove)),
        'avg_annual_dividends': lambda: (sum(dividend['price'] for dividend in json_dividends_data) / len(json_dividends_data)) if json_dividends_data else None,
        'avg_price': lambda: None,
        'beta': lambda: None,
        'cagr_profit': lambda: None,
        'cagr_revenue': lambda: None,
        'debit': lambda: None,
        'dy': lambda: text_to_number(get_substring(html_page, 'DY</span>', '</span>', patterns_to_remove)),
        'ebit': lambda: None,
        'enterprise_value': lambda: None,
        'equity_price': lambda: None,
        'equity_value': lambda: None,
        'gross_margin': lambda: None,
        'initial_date': lambda: None,
        'latests_dividends': lambda: get_leatests_dividends(json_dividends_data),
        'link': lambda: None,
        'liquidity': lambda: None,
        'management_fee': lambda: None,
        'market_value': lambda: None,
        'max_52_weeks': lambda: None,
        'min_52_weeks': lambda: None,
        'name': lambda: remove_type_from_name(get_substring(html_page, 'name-company">', '<', patterns_to_remove).replace('&amp;', '&')),
        'net_margin': lambda: None,
        'net_profit': lambda: None,
        'net_revenue': lambda: None,
        'payout': lambda: None,
        'pl': lambda: None,
        'price': lambda: text_to_number(get_substring(html_page, '<span class="value">US$', '</span>', patterns_to_remove)),
        'pvp': lambda: None,
        'roe': lambda: None,
        'roic': lambda: None,
        'sector': lambda: None,
        'total_issued_shares': lambda: None,
        'total_real_state': lambda: None,
        'type': lambda: 'ETF',
        'vacancy': lambda: None,
        'variation_12m': lambda: text_to_number(get_substring(html_page, 'VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove)),
        'variation_30d': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def get_etf_from_investidor10(ticker, info_names):
    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': 'https://investidor10.com.br/etfs-global/voo',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0',
        }

        response = request_get(f'https://investidor10.com.br/etfs-global/{ticker}', headers)
        html_page =  response.text[15898:]

        id = get_substring(html_page, 'etfId" value="', '"')

        json_dividends_data = {}
        if 'latests_dividends' in info_names or 'avg_annual_dividends' in info_names:
            response = request_get(f'https://investidor10.com.br/api/etfs/dividendos/chart/{id}/1825/ano', headers)
            json_dividends_data = response.json()

        converted_data = convert_investidor10_etf_data(html_page, json_dividends_data, info_names)
        log_debug(f'Converted fresh Investidor 10 data: {converted_data}')
        return converted_data
    except Exception as error:
        log_error(f'Error fetching data from Investidor 10 for "{ticker}": {traceback.format_exc()}')
        return None

def convert_stockanalysis_etf_data(html_page, json_quote_data, info_names):
    def get_leatests_dividends(html_page):
        try:
          paid_dividends = get_substring(html_page, 'dividendTable:[', '],')

          splitted_paid_dividends = paid_dividends.split('},')

          paid_dividends_by_date = { datetime.strptime(get_substring(dividend_data, 'dt:"', '",'), '%Y-%m-%d') : text_to_number(get_substring(dividend_data, 'amt:', ',')) for dividend_data in splitted_paid_dividends }

          newest_dividend = max(paid_dividends_by_date)

          return paid_dividends_by_date[newest_dividend]
        except:
          return None

    def get_avg_price(json_quote_data):
        latest_quotes = json_quote_data['data'][-200:]
        return sum([ item[1] for item in latest_quotes ]) / len(latest_quotes)

    equity_value = multiply_by_unit(get_substring(html_page, 'aum:"$', '",'))
    total_issued_shares = multiply_by_unit(get_substring(html_page, 'sharesOut:"', '",'))
    equity_price = equity_value / total_issued_shares
    price = text_to_number(get_substring(html_page, 'cl:', ','))

    ALL_INFO = {
        'actuation': lambda: get_substring(html_page, '"Index Tracked","', '"]'),
        'assets_value': lambda: None,
        'avg_annual_dividends': lambda: text_to_number(get_substring(html_page, 'dps:"$', '",')) / 12,
        'avg_price': lambda: get_avg_price(json_quote_data),
        'beta': lambda: text_to_number(get_substring(html_page, 'beta:"', '",')),
        'cagr_profit': lambda: None,
        'cagr_revenue': lambda: None,
        'debit': lambda: None,
        'dy': lambda: text_to_number(get_substring(html_page, 'dividendYield:"', '%",')),
        'ebit': lambda: None,
        'enterprise_value': lambda: None,
        'equity_price': lambda: equity_price,
        'equity_value': lambda: equity_value,
        'gross_margin': lambda: None,
        'initial_date': lambda: get_substring(html_page, 'inception:"', '",'),
        'latests_dividends': lambda: get_leatests_dividends(html_page),
        #'latests_dividends': lambda: text_to_number(get_substring(html_page, 'dps:"$', '",')),
        'link': lambda: get_substring(html_page, 'etf_website:"', '",'),
        'liquidity': lambda: text_to_number(get_substring(html_page, 'v:', ',')),
        'management_fee': lambda: text_to_number(get_substring(html_page, 'expenseRatio:"', '%",')),
        'market_value': lambda: None,
        'max_52_weeks': lambda: text_to_number(get_substring(html_page, 'h52:', ',')),
        'min_52_weeks': lambda: text_to_number(get_substring(html_page, 'l52:', ',')),
        'name': lambda: remove_type_from_name(get_substring(html_page, 'name:"', '",')),
        'net_margin': lambda: None,
        'net_profit': lambda: None,
        'net_revenue': lambda: None,
        'payout': lambda: text_to_number(get_substring(html_page, 'payoutRatio:"', '%",')),
        'pl': lambda: text_to_number(get_substring(html_page, 'peRatio:"', '",')),
        'price': lambda: price,
        'pvp': lambda: price / equity_price,
        'roe': lambda: None,
        'roic': lambda: None,
        'sector': lambda: get_substring(html_page, '"Asset Class","', '"]') + '/' + get_substring(html_page, '"Category","', '"]'),
        'total_issued_shares': lambda: total_issued_shares,
        'total_real_state': lambda: None,
        'type': lambda: 'ETF',
        'vacancy': lambda: None,
        'variation_12m': lambda: text_to_number(get_substring(html_page, 'ch1y:"', '",')),
        'variation_30d': lambda: None
    }

    final_data = { info: ALL_INFO[info]() for info in info_names }

    return final_data

def get_etf_from_stockanalysis(ticker, info_names):
    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'referer': 'https://stockanalysis.com/',
            'upgrade-insecure-requests': '1',
            'priority': 'u=0, i',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 OPR/118.0.0.0',
        }

        response = request_get(f'https://stockanalysis.com/etf/{ticker}', headers)
        html_page = get_substring(response.text[5_000:], 'Promise.all([', 'news:')

        response = request_get(f'https://stockanalysis.com/api/symbol/e/{ticker}/history?type=chart', headers)
        json_quote_data = response.json()

        converted_data = convert_stockanalysis_etf_data(html_page, json_quote_data, info_names)
        log_debug(f'Converted fresh Stock Analysis data: {converted_data}')
        return converted_data
    except Exception as error:
        log_error(f'Error fetching data from Stock Analysis for "{ticker}": {traceback.format_exc()}')
        return None

def get_etf_from_all_sources(ticker, info_names):
    data_stockanalysis = get_etf_from_stockanalysis(ticker, info_names)
    log_info(f'Data from Stock Analysis: {data_stockanalysis}')

    missing_stockanalysis_infos = filter_remaining_infos(data_stockanalysis, info_names)
    log_debug(f'Missing info from Stock Analysis: {missing_stockanalysis_infos}')

    if data_stockanalysis and not missing_stockanalysis_infos:
        return data_stockanalysis

    data_investidor_10 = get_etf_from_investidor10(ticker, missing_stockanalysis_infos or info_names)
    log_info(f'Data from Investidor 10: {data_investidor_10}')

    if not data_investidor_10:
        return data_stockanalysis

    return { **data_stockanalysis, **data_investidor_10 }

def get_etf_from_sources(ticker, share_type, source, info_names):
    SOURCES = {
        VALID_SOURCES['STOCKANALYSIS_SOURCE']: get_etf_from_stockanalysis,
        VALID_SOURCES['INVESTIDOR10_SOURCE']: get_etf_from_investidor10
    }

    fetch_function = SOURCES.get(source, get_etf_from_all_sources)
    return fetch_function(ticker, info_names)

def get_data_from_cache(ticker, info_names, can_use_cache):
    if not can_use_cache:
        return None

    cached_data = read_cache(ticker)
    if not cached_data:
        return None

    filtered_data = { key: cached_data[key] for key in info_names if key in cached_data }
    log_info(f'Data from Cache: {filtered_data}')

    return filtered_data

def get_data(ticker, share_type, source, info_names, can_use_cache, get_data_from_sources):
    cached_data = get_data_from_cache(ticker, info_names, can_use_cache)

    SHOULD_UPDATE_CACHE = True

    if not can_use_cache:
        return not SHOULD_UPDATE_CACHE, get_data_from_sources(ticker, share_type, source, info_names)

    missing_cache_info_names = filter_remaining_infos(cached_data, info_names)

    if not missing_cache_info_names:
        return not SHOULD_UPDATE_CACHE, cached_data

    source_data = get_data_from_sources(ticker, share_type, source, missing_cache_info_names)

    if cached_data and source_data:
        return SHOULD_UPDATE_CACHE, { **cached_data, **source_data }
    elif cached_data and not source_data:
        return not SHOULD_UPDATE_CACHE, cached_data
    elif not cached_data and source_data:
        return SHOULD_UPDATE_CACHE, source_data

    return not SHOULD_UPDATE_CACHE, None

def get_parameter_info(params, name, default=None):
    return params.get(name, default).replace(' ', '').lower()

def get_cache_parameter_info(params, name, default='0'):
    return get_parameter_info(params, name, default) in { '1', 's', 'sim', 't', 'true', 'y', 'yes' }

@app.route('/reit/<ticker>', methods=['GET'])
def get_reit_data(ticker):
    return get_share_data(ticker, 'reits', get_stock_or_reit_from_sources)

@app.route('/stock/<ticker>', methods=['GET'])
def get_stock_data(ticker):
    return get_share_data(ticker, 'stocks', get_stock_or_reit_from_sources)

@app.route('/etf/<ticker>', methods=['GET'])
def get_etf_data(ticker):
    return get_share_data(ticker, '', get_etf_from_sources)

def get_share_data(ticker, share_type, get_data_from_sources):
    should_delete_all_cache = get_cache_parameter_info(request.args, 'should_delete_all_cache')
    should_clear_cached_data = get_cache_parameter_info(request.args, 'should_clear_cached_data')
    should_use_cache = get_cache_parameter_info(request.args, 'should_use_cache', '1')

    ticker = ticker.upper()

    raw_source = get_parameter_info(request.args, 'source', VALID_SOURCES['ALL_SOURCE'])
    source = raw_source if raw_source in VALID_SOURCES.values() else VALID_SOURCES['ALL_SOURCE']

    raw_info_names = [ info for info in get_parameter_info(request.args, 'info_names', '').split(',') if info in VALID_INFOS ]
    info_names = raw_info_names if len(raw_info_names) else VALID_INFOS

    log_debug(f'Should Delete cache? {should_delete_all_cache} - Should Clear cache? {should_clear_cached_data} - Should Use cache? {should_use_cache}')
    log_debug(f'Ticker: {ticker} - Source: {source} - Info names: {info_names}')

    can_use_cache = preprocess_cache(ticker, should_delete_all_cache, should_clear_cached_data, should_use_cache)

    should_update_cache, data = get_data(ticker, share_type, source, info_names, can_use_cache, get_data_from_sources)

    log_debug(f'Final Data: {data}')

    if not data:
        return jsonify({ 'error': 'No data found' }), 404

    if can_use_cache and should_update_cache:
        upsert_cache(ticker, data)

    return jsonify(data), 200

if __name__ == '__main__':
    log_debug('Starting stockCrawler API')
    app.run(debug=LOG_LEVEL == 'DEBUG')
