import ast
from datetime import datetime, timedelta
from hashlib import sha512
import json
import os
import re
import traceback

from flask import Flask, jsonify, request

import requests
from requests import RequestException

app = Flask(__name__)
app.json.sort_keys = False

TRUE_BOOL_VALUES = ('1', 's', 'sim', 'y', 'yes', 't', 'true')

CACHE_FILE = '/tmp/cache.txt'
CACHE_EXPIRY = timedelta(days=1)

VALID_SOURCES = {
    'STOCKANALYSIS_SOURCE': 'stockanalysis',
    'INVESTIDOR10_SOURCE': 'investidor10',
    'ALL_SOURCE': 'all'
}

VALID_INFOS = [ 'actuation', 'assets_value', 'avg_annual_dividends', 'cagr_profit', 'cagr_revenue', 'debit', 'dy', 'ebit', 'enterprise_value', 'equity_value', 'equity_price', 'gross_margin', 'initial_date', 'latests_dividends', 'link', 'liquidity', 'management_fee', 'market_value', 'max_52_weeks', 'min_52_weeks', 'name', 'net_margin', 'net_profit', 'net_revenue', 'payout', 'pl', 'price', 'pvp', 'roe', 'sector', 'total_issued_shares', 'type', 'variation_12m', 'variation_30d', 'beta' ]

def request_get(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    #print(f'Response from {url} : {response}')

    return response

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

def delete_cache():
    if os.path.exists(CACHE_FILE):
        #print('Deleting cache')
        os.remove(CACHE_FILE)
        #print('Deleted')

def clear_cache(hash_id):
    #print('Cleaning cache')
    with open(CACHE_FILE, 'w+') as cache_file:
        lines = cache_file.readlines()

        for line in lines:
            if not line.startswith(hash_id):
                cache_file.write(line)
   #print('Cleaned')

def read_cache(hash_id, should_clear_cache):
    if not os.path.exists(CACHE_FILE):
        return None, None

    if should_clear_cache:
        clear_cache(hash_id)
        return None, None

    control_clean_cache = False

    #print('Reading cache')
    with open(CACHE_FILE, 'r') as cache_file:
        for line in cache_file:
            if not line.startswith(hash_id):
                continue

            _, cached_datetime, data = line.strip().split('#@#')

            cached_date = datetime.strptime(cached_datetime, '%Y-%m-%d %H:%M:%S')

            #print(f'Found value: Date: {cached_datetime} - Data: {data}')
            if datetime.now() - cached_date <= CACHE_EXPIRY:
                #print('Finished read')
                return ast.literal_eval(data), cached_date

            control_clean_cache = True
            break

    if control_clean_cache:
        clear_cache(hash_id)

    return None, None

def write_to_cache(hash_id, data):
    #print('Writing cache')
    with open(CACHE_FILE, 'a') as cache_file:
        #print(f'Writed value: {f'{hash_id}#@#{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}#@#{data}\n'}')
        cache_file.write(f'{hash_id}#@#{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}#@#{data}\n')
    #print('Writed')

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
        'name': lambda: remove_type_from_name(json_ticker_page['company_name']),
        'type': lambda: json_ticker_page['type'],
        'sector': lambda: json_ticker_page['industry']['sector']['name'],
        'actuation': lambda: json_ticker_page['industry']['name'],
        'link': lambda: None,
        'price': lambda: actual_price,
        'liquidity': lambda: balance['volume_avg'],
        'total_issued_shares': lambda: balance['shares_outstanding'],
        'enterprise_value': lambda: None,
        'equity_value': lambda: balance['total_equity'],
        'equity_price': lambda: None,
        'net_revenue': lambda: balance['revenue'],
        'net_profit': lambda: balance['net_income'],
        'net_margin': lambda: text_to_number(balance['net_margin']),
        'gross_margin': lambda: text_to_number(balance['gross_margin']),
        'cagr_revenue': lambda: balance['growth_net_revenue_last_5_years'],
        'cagr_profit': lambda: balance['growth_net_profit_last_5_years'],
        'debit': lambda: text_to_number(balance['long_term_debt']),
        'ebit':  lambda: text_to_number(balance['ebit']),
        'variation_12m': lambda: balance['variation_year'],
        'variation_30d': lambda: None,
        'min_52_weeks': lambda: None,
        'max_52_weeks': lambda: None,
        'pvp': lambda: text_to_number(balance['pvp']),
        'dy':  lambda: text_to_number(balance['dy']),
        'latests_dividends': lambda: get_leatests_dividends(json_dividends_data),
        'avg_annual_dividends': lambda: (sum(dividend['price'] for dividend in json_dividends_data) / len(json_dividends_data)) if json_dividends_data else None,
        'vacancy': lambda: None,
        'total_real_state': lambda: None,
        'assets_value': lambda: balance['total_assets'],
        'market_value': lambda: balance['market_cap'],
        'initial_date': lambda: json_ticker_page['start_year_on_stock_exchange'],
        'pl': lambda: text_to_number(balance['pl']),
        'roe': lambda: text_to_number(balance['roe']),
        'payout': lambda: text_to_number(balance['api_info']['common_size_ratios']['dividend_payout_ratio']),
        'beta': lambda: None,
        'management_fee': lambda: None
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

        #print(f'Converted Investidor 10 data: {convert_investidor10_stock_or_reit_data(json_ticker_page, json_dividends_data, info_names)}')
        return convert_investidor10_stock_or_reit_data(json_ticker_page, json_dividends_data, info_names)
    except Exception as error:
        #print(f"Error on get Investidor 10 data: {traceback.format_exc()}")
        return None

def convert_stockanalysis_stock_or_reit_data(ticker, share_type, generic_json, statistics_json, info_names):
    roa = text_to_number(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['financialEfficiency']]['data']][1]]['value']])
    net_profit = multiply_by_unit(generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][0]['netIncome']])

    ALL_INFO = {
        'name': lambda: generic_json['nodes'][1]['data'][generic_json['nodes'][1]['data'][1]['nameFull']],
        #'type': lambda: generic_json['nodes'][1]['data'][generic_json['nodes'][1]['data'][1]['type']],
        'type': lambda: share_type[:-1].upper(),
        'sector': lambda: generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][0]['infoTable']][1]]['v']],
        'actuation': lambda: generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][0]['infoTable']][0]]['v']],
        #'link': lambda: generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][0]['infoTable']][6]]['eu']],
        'link': lambda: f'https://stockanalysis.com/stocks/{ticker}/company/',
        'price': lambda: generic_json['nodes'][1]['data'][generic_json['nodes'][1]['data'][generic_json['nodes'][1]['data'][1]['quote']]['cl']],
        #'liquidity': lambda: generic_json['nodes'][1]['data'][generic_json['nodes'][1]['data'][generic_json['nodes'][1]['data'][1]['quote']]['v']],
        'liquidity': lambda: text_to_number(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['stockPrice']]['data']][5]]['value']]),
        'total_issued_shares': lambda: multiply_by_unit(generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][0]['sharesOut']]),
        'enterprise_value': lambda: multiply_by_unit(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['valuation']]['data']][1]]['value']]),
        'equity_value': lambda: None,
        'equity_price': lambda: None,
        'net_revenue': lambda: multiply_by_unit(generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][0]['revenue']]),
        'net_profit': lambda: net_profit,
        'net_margin': lambda: multiply_by_unit(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['margins']]['data']][3]]['value']]),
        'gross_margin': lambda: multiply_by_unit(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['margins']]['data']][0]]['value']]),
        'cagr_revenue': lambda: None,
        'cagr_profit': lambda: None,
        'debit': lambda: multiply_by_unit(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['balanceSheet']]['data']][1]]['value']]),
        'ebit':  lambda: multiply_by_unit(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['incomeStatement']]['data']][2]]['value']]),
        'variation_12m': lambda: text_to_number(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['stockPrice']]['data']][1]]['value']]),
        'variation_30d': lambda: None,
        'min_52_weeks': lambda: generic_json['nodes'][1]['data'][generic_json['nodes'][1]['data'][generic_json['nodes'][1]['data'][1]['quote']]['l52']],
        'max_52_weeks': lambda: generic_json['nodes'][1]['data'][generic_json['nodes'][1]['data'][generic_json['nodes'][1]['data'][1]['quote']]['h52']],
        'pvp': lambda: None,
        'dy':  lambda: text_to_number(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['dividends']]['data']][1]]['value']]),
        'latests_dividends': lambda: None,
        'avg_annual_dividends': lambda: text_to_number(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['dividends']]['data']][0]]['value']]),
        'vacancy': lambda: None,
        'total_real_state': lambda: None,
        'assets_value': lambda: net_profit / roa,
        'market_value': lambda: multiply_by_unit(generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][0]['marketCap']]),
        #'initial_date': lambda: generic_json['nodes'][1]['data'][generic_json['nodes'][1]['data'][1]['ipoDate']],
        'initial_date': lambda: None,
        'pl': lambda: generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][0]['peRatio']],
        'roe': lambda: text_to_number(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['financialEfficiency']]['data']][0]]['value']]),
        'payout': lambda: text_to_number(statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][statistics_json['nodes'][2]['data'][0]['dividends']]['data']][4]]['value']]),
        'beta': lambda: generic_json['nodes'][2]['data'][generic_json['nodes'][2]['data'][0]['beta']],
        'management_fee': lambda: None
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

        response = request_get(f'https://stockanalysis.com/stocks/{ticker}/__data.json?x-sveltekit-trailing-slash=1&x-sveltekit-invalidated=001', headers)
        generic_json = response.json()

        response = request_get(f'https://stockanalysis.com/stocks/{ticker}/statistics/__data.json?x-sveltekit-trailing-slash=1&x-sveltekit-invalidated=001', headers)
        statistics_json = response.json()

        print(f'generic_json: {generic_json}')
        print(f'statistics_json: {statistics_json}')
        #print(f'Converted Stock Analysis data: {convert_stockanalysis_stock_or_reit_data(ticker, generic_json, statistics_json, info_names)}')
        return convert_stockanalysis_stock_or_reit_data(ticker, share_type, generic_json, statistics_json, info_names)
    except Exception as error:
        print(f"Error on get Stock Analysis data: {traceback.format_exc()}")
        return None

def get_stock_or_reit_from_all_sources(ticker, share_type, info_names):
    data_stockanalysis = get_stock_or_reit_from_stockanalysis(ticker, share_type, info_names)
    #print(f'Data from Stock Analysis: {data_stockanalysis}')

    blank_stockanalysis_info_names = [ info for info in info_names if not data_stockanalysis.get(info, False) ]
    #print(f'Info names: {blank_stockanalysis_info_names}')

    if data_stockanalysis and not blank_stockanalysis_info_names:
        return data_stockanalysis

    data_investidor10 = get_stock_or_reit_from_investidor10(ticker, share_type, blank_stockanalysis_info_names if blank_stockanalysis_info_names else info_names)
    #print(f'Data from Investidor10: {data_investidor10}')

    if not data_investidor10:
        return data_stockanalysis

    return { **data_stockanalysis, **data_investidor10 }

def get_stock_or_reit_from_sources(ticker, share_type, source, info_names):
    if source == VALID_SOURCES['STOCKANALYSIS_SOURCE']:
        return get_stock_or_reit_from_stockanalysis(ticker, share_type, info_names)
    elif source == VALID_SOURCES['INVESTIDOR10_SOURCE']:
        return get_stock_or_reit_from_investidor10(ticker, share_type, info_names)

    return get_stock_or_reit_from_all_sources(ticker, share_type, info_names)

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
        'name': lambda: remove_type_from_name(get_substring(html_page, 'name-company">', '<', patterns_to_remove).replace('&amp;', '&')),
        'type': lambda: 'ETF',
        'sector': lambda: None,
        'actuation': lambda: None,
        'link': lambda: None,
        'price': lambda: text_to_number(get_substring(html_page, '<span class="value">US$', '</span>', patterns_to_remove)),
        'liquidity': lambda: None,
        'total_issued_shares': lambda: None,
        'enterprise_value': lambda: None,
        'equity_value': lambda: None,
        'equity_price': lambda: None,
        'net_revenue': lambda: None,
        'net_profit': lambda: None,
        'net_margin': lambda: None,
        'gross_margin': lambda: None,
        'cagr_revenue': lambda: None,
        'cagr_profit': lambda: None,
        'debit': lambda: None,
        'ebit':  lambda: None,
        'variation_12m': lambda: text_to_number(get_substring(html_page, 'VARIAÇÃO (12M)</span>', '</span>', patterns_to_remove)),
        'variation_30d': lambda: None,
        'min_52_weeks': lambda: None,
        'max_52_weeks': lambda: None,
        'pvp': lambda: None,
        'dy':  lambda: text_to_number(get_substring(html_page, 'DY</span>', '</span>', patterns_to_remove)),
        'latests_dividends': lambda: get_leatests_dividends(json_dividends_data),
        'avg_annual_dividends': lambda: (sum(dividend['price'] for dividend in json_dividends_data) / len(json_dividends_data)) if json_dividends_data else None,
        'vacancy': lambda: None,
        'total_real_state': lambda: None,
        'assets_value': lambda: multiply_by_unit(get_substring(html_page, 'Capitalização</span>', '</span>', patterns_to_remove)),
        'market_value': lambda: None,
        'initial_date': lambda: None,
        'pl': lambda: None,
        'roe': lambda: None,
        'payout': lambda: None,
        'beta': lambda: None,
        'management_fee': lambda: None
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

        #print(f'Converted Investidor 10 data: {convert_investidor10_etf_data(html_page, json_dividends_data, info_names)}')
        return convert_investidor10_etf_data(html_page, json_dividends_data, info_names)
    except Exception as error:
        #print(f"Error on get Investidor 10 data: {traceback.format_exc()}")
        return None

def convert_stockanalysis_etf_data(html_page, info_names):
    def get_leatests_dividends(html_page):
        try:
          paid_dividends = get_substring(html_page, 'dividendTable:[', '],')

          splitted_paid_dividends= paid_dividends.split('},')

          paid_dividends_by_date = { datetime.strptime(get_substring(dividend_data, 'dt:"', '",'), '%Y-%m-%d') : text_to_number(get_substring(dividend_data, 'amt:', ',')) for dividend_data in splitted_paid_dividends }

          newest_dividend = max(paid_dividends_by_date)

          return paid_dividends_by_date[newest_dividend]
        except:
          return None

    equity_value = multiply_by_unit(get_substring(html_page, 'aum:"$', '",'))
    total_issued_shares = multiply_by_unit(get_substring(html_page, 'sharesOut:"', '",'))
    equity_price = equity_value / total_issued_shares
    price = text_to_number(get_substring(html_page, 'cl:', ','))

    ALL_INFO = {
        'name': lambda: remove_type_from_name(get_substring(html_page, 'name:"', '",')),
        'type': lambda: 'ETF',
        'sector': lambda: get_substring(html_page, '"Asset Class","', '"]') + '/' + get_substring(html_page, '"Category","', '"]'),
        'actuation': lambda: get_substring(html_page, '"Index Tracked","', '"]'),
        'link': lambda: get_substring(html_page, 'etf_website:"', '",'),
        'price': lambda: price,
        'liquidity': lambda: text_to_number(get_substring(html_page, 'v:', ',')),
        'total_issued_shares': lambda: total_issued_shares,
        'enterprise_value': lambda: None,
        'equity_value': lambda: equity_value,
        'equity_price': lambda: equity_price,
        'net_revenue': lambda: None,
        'net_profit': lambda: None,
        'net_margin': lambda: None,
        'gross_margin': lambda: None,
        'cagr_revenue': lambda: None,
        'cagr_profit': lambda: None,
        'debit': lambda: None,
        'ebit':  lambda: None,
        'variation_12m': lambda: text_to_number(get_substring(html_page, 'ch1y:"', '",')),
        'variation_30d': lambda: None,
        'min_52_weeks': lambda: text_to_number(get_substring(html_page, 'l52:', ',')),
        'max_52_weeks': lambda: text_to_number(get_substring(html_page, 'h52:', ',')),
        'pvp': lambda: price / equity_price,
        'dy':  lambda: text_to_number(get_substring(html_page, 'dividendYield:"', '%",')),
        'latests_dividends': lambda: get_leatests_dividends(html_page),
        'avg_annual_dividends': lambda: text_to_number(get_substring(html_page, 'dps:"$', '",')) / 12,
        'vacancy': lambda: None,
        'total_real_state': lambda: None,
        'assets_value': lambda: None,
        'market_value': lambda: None,
        'initial_date': lambda: get_substring(html_page, 'inception:"', '",'),
        'pl': lambda: text_to_number(get_substring(html_page, 'peRatio:"', '",')),
        'roe': lambda: None,
        'payout': lambda: text_to_number(get_substring(html_page, 'payoutRatio:"', '%",')),
        'beta': lambda: text_to_number(get_substring(html_page, 'beta:"', '",')),
        'management_fee': lambda: text_to_number(get_substring(html_page, 'expenseRatio:"', '%",')),
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
        json_data = get_substring(response.text[10_000:], 'const data =', 'news:')

        #print(f'Converted Stock Analysis data: {convert_stockanalysis_etf_data(json_data, info_names)}')
        return convert_stockanalysis_etf_data(json_data, info_names)
    except Exception as error:
        #print(f"Error on get Stock Analysis data: {traceback.format_exc()}")
        return None

def get_etf_from_all_sources(ticker, info_names):
    data_stockanalysis = get_etf_from_stockanalysis(ticker, info_names)
    #print(f'Data from Stock Analysis: {data_stockanalysis}')

    blank_stockanalysis_info_names = [ info for info in info_names if not data_stockanalysis.get(info, False) ]
    #print(f'Info names: {blank_stockanalysis_info_names}')

    if data_stockanalysis and not blank_stockanalysis_info_names:
        return data_stockanalysis

    data_investidor10 = get_etf_from_investidor10(ticker, blank_stockanalysis_info_names if blank_stockanalysis_info_names else info_names)
    #print(f'Data from Investidor10: {data_investidor10}')

    if not data_investidor10:
        return data_stockanalysis

    return { **data_stockanalysis, **data_investidor10 }

def get_etf_from_sources(ticker, share_type, source, info_names):
    if source == VALID_SOURCES['STOCKANALYSIS_SOURCE']:
        return get_etf_from_stockanalysis(ticker, info_names)
    elif source == VALID_SOURCES['INVESTIDOR10_SOURCE']:
        return get_etf_from_investidor10(ticker, info_names)

    return get_etf_from_all_sources(ticker, info_names)

@app.route('/reit/<ticker>', methods=['GET'])
def get_reit_data(ticker):
    return get_share_data(ticker, 'reits', get_stock_or_reit_from_sources)

@app.route('/stock/<ticker>', methods=['GET'])
def get_stock_data(ticker):
    return get_share_data(ticker, 'stocks', get_stock_or_reit_from_sources)

@app.route('/etf/<ticker>', methods=['GET'])
def get_etf_data(ticker):
    return get_share_data(ticker, '', get_etf_from_sources)

def get_share_data(ticker, share_type, request_share):
    should_delete_cache = request.args.get('should_delete_cache', '0').lower() in TRUE_BOOL_VALUES
    should_clear_cache = request.args.get('should_clear_cache', '0').lower() in TRUE_BOOL_VALUES
    should_use_cache = request.args.get('should_use_cache', '1').lower() in TRUE_BOOL_VALUES

    source = request.args.get('source', VALID_SOURCES['ALL_SOURCE']).replace(' ', '').lower()
    source = source if source in VALID_SOURCES.values() else VALID_SOURCES['ALL_SOURCE']

    info_names = request.args.get('info_names', '').replace(' ', '').lower().split(',')
    info_names = [ info for info in info_names if info in VALID_INFOS ]
    info_names = info_names if len(info_names) else VALID_INFOS

    #print(f'Delete cache? {should_delete_cache}, Clear cache? {should_clear_cache}, Use cache? {should_use_cache}')
    #print(f'Ticker: {ticker}, Source: {source}, Info names: {info_names}, Share Type: {share_type}')

    if should_delete_cache:
        delete_cache()

    should_use_and_not_delete_cache = should_use_cache and not should_delete_cache

    if should_use_and_not_delete_cache:
        id = f'{ticker}{source}{",".join(sorted(info_names))}'.encode('utf-8')
        hash_id = sha512(id).hexdigest()
        #print(f'Cache Hash ID: {hash_id}, From values: {id}')

        cached_data, cache_date = read_cache(hash_id, should_clear_cache)

        if cached_data:
            #print(f'Data from Cache: {cached_data}')
            return jsonify({'data': cached_data, 'source': 'cache', 'date': cache_date.strftime("%d/%m/%Y, %H:%M")}), 200

    data = request_share(ticker, share_type, source, info_names)
    #print(f'Data from Source: {data}')

    if should_use_and_not_delete_cache and not should_clear_cache:
        write_to_cache(hash_id, data)

    return jsonify({'data': data, 'source': 'fresh', 'date': datetime.now().strftime("%d/%m/%Y, %H:%M")}), 200

if __name__ == '__main__':
    is_debug = os.getenv('IS_DEBUG', False)
    app.run(debug=is_debug)
