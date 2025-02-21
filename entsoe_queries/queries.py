# entsoe_queries/queries.py

import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pytz
import time

########################### aFRR VOLUMES ###########################

def query_activated_aFRR_Volume(client_raw, start, end):
    """
    Query activated aFRR Volume data from the ENTSOE API.
    
    Parameters:
        client_raw: An instance with a method _base_request to call the API.
        start (datetime): Start time for the query.
        end (datetime): End time for the query.
        
    Returns:
        DataFrame with processed aFRR+ and aFRR- volumes.
    """
    params = {
        'documentType': 'A83',
        'ProcessType': 'A51',
        'businessType': 'A96',
        'ControlArea_Domain': '10YCH-SWISSGRIDZ',
        # 'ControlArea_Domain': '10YBE----------2',
        'Direction': 'A01',
    }
    response = client_raw._base_request(params=params, start=start, end=end)
    root = ET.fromstring(response.text)
    ns = {'ns': 'urn:iec62325.351:tc57wg16:451-6:balancingdocument:3:0'}
    
    all_info = pd.DataFrame(extract_time_direction_quantity(root, ns))
    result = post_process_aFRR(all_info, start)

    result['aFRR+ Volume'] = pd.to_numeric(result['aFRR+ Volume'])
    result['aFRR- Volume'] = pd.to_numeric(result['aFRR- Volume'])
    return result


def post_process_aFRR(all_info, start):
    """
    Post-process the raw aFRR data by merging upward and downward flow data.
    
    Parameters:
        all_info (DataFrame): Data extracted from the API.
        start (datetime): Start time for aligning the time intervals.
    
    Returns:
        DataFrame indexed by time with separate columns for upward (aFRR+) and downward (aFRR-) volumes.
    """
    up_info = (all_info[all_info['flow_direction'] == 'A01']
               .reset_index(drop=True)
               .rename(columns={'quantity': 'aFRR+ Volume'}))
    down_info = (all_info[all_info['flow_direction'] == 'A02']
                 .reset_index(drop=True)
                 .rename(columns={'quantity': 'aFRR- Volume'}))

    if len(up_info) == 96 and len(down_info) == 96:
        start_time = start
        intervals = pd.date_range(start=start_time, periods=96, freq='15min')
        down_info['period_start_time'] = intervals
        up_info['period_start_time'] = intervals
        result = pd.merge(
            up_info[['period_start_time', 'aFRR+ Volume']],
            down_info[['period_start_time', 'aFRR- Volume']],
            on='period_start_time',
            how='inner'
        ).set_index('period_start_time')
    else:
        print('Something is not working as expected with the aFRR volumes data.')
        result = None

    return result


def extract_time_direction_quantity(root, ns):
    """
    Extract time, direction, and quantity data from the XML response.
    
    Parameters:
        root: XML root element.
        ns (dict): Namespace dictionary for parsing XML.
    
    Returns:
        List of dictionaries with keys: flow_direction, period_start_time, and quantity.
    """
    info = []
    for time_series in root.findall('.//ns:TimeSeries', ns):
        flow_direction = time_series.find('ns:flowDirection.direction', ns).text
        start_time_local = convert_to_local_time(
            time_series.find('.//ns:Period/ns:timeInterval/ns:start', ns).text
        )
        for point in time_series.findall('.//ns:Point', ns):
            quantity = point.find('ns:quantity', ns).text
            info.append({
                'flow_direction': flow_direction,
                'period_start_time': start_time_local,
                'quantity': quantity
            })
    return info


########################### aFRR BIDS ###########################

def query_aFRR_all_bids(client_raw, start, end):
    """
    Query all aFRR bids in 12-hour windows, handling pagination via offset.
    
    Parameters:
        client_raw: An instance with a method _base_request to call the API.
        start (datetime): Start time for the query.
        end (datetime): End time for the query.
        
    Returns:
        DataFrame containing all queried bids.
    """
    all_results = pd.DataFrame()
    current_start = start
    current_end = start + timedelta(hours=12)
    
    while current_start < end:
        if current_end > end:
            current_end = end
        
        offset = 0
        while True:
            try:
                result = query_aFRR_100_Bids(client_raw, current_start, current_end, offset)
                if result.empty:
                    break
                all_results = pd.concat([all_results, result], ignore_index=True)
                offset += 100
                time.sleep(2)  # Delay to avoid hitting rate limits
                print(f"Current window: {current_start} to {current_end}, downloaded: {offset} bids")
            except Exception as e:
                print(f"Error encountered at length {len(all_results)} for window {current_start} to {current_end}: {e}")
                break
        
        current_start += timedelta(hours=12)
        current_end += timedelta(hours=12)
    
    return all_results


def query_aFRR_100_Bids(client_raw, start, end, offset):
    """
    Query a batch of 100 aFRR bids from the ENTSOE API.
    
    Parameters:
        client_raw: An instance with a method _base_request.
        start (datetime): Start time for the query.
        end (datetime): End time for the query.
        offset (int): Offset for pagination.
        
    Returns:
        DataFrame containing the bids for the given offset.
    """
    params = {
        'documentType': 'A37',
        'ProcessType': 'A51',
        'businessType': 'B74',
        'connecting_Domain': '10YCH-SWISSGRIDZ',
        'Direction': 'A01',
        'Offset': offset
    }
    response = client_raw._base_request(params=params, start=start, end=end)
    print(response.text)
    root = ET.fromstring(response.text)
    ns = {'ns': 'urn:iec62325.351:tc57wg16:451-7:reservebiddocument:7:1'}
    result = pd.DataFrame(extract_bids(root, ns))
    return result


def extract_bids(root, ns):
    """
    Extract bid offers from the XML response.
    
    Parameters:
        root: XML root element.
        ns (dict): Namespace dictionary for parsing XML.
    
    Returns:
        List of dictionaries with bid details.
    """
    offers = []
    for time_series in root.findall('.//ns:Bid_TimeSeries', ns):
        bid_id = time_series.find('ns:mRID', ns).text
        start_time_local = convert_to_local_time(
            time_series.find('.//ns:timeInterval/ns:start', ns).text
        )
        end_time_local = convert_to_local_time(
            time_series.find('.//ns:timeInterval/ns:end', ns).text
        )
        for point in time_series.findall('.//ns:Point', ns):
            quantity = point.find('ns:quantity.quantity', ns).text
            price = point.find('ns:energy_Price.amount', ns).text
            offers.append({
                'bid_id': bid_id,
                'start_time': start_time_local,
                'end_time': end_time_local,
                'quantity': quantity,
                'price': price
            })
    return offers


def post_process(aFRR_bids):
    """
    Post-process the aFRR bids data to calculate cumulative volumes.
    
    Parameters:
        aFRR_bids (DataFrame): Raw bids data.
        
    Returns:
        DataFrame with cumulative volumes and prices grouped by start time.
    """
    start_times = []
    cumulative_volumes = []
    prices = []
    
    aFRR_bids['quantity'] = pd.to_numeric(aFRR_bids['quantity'])
    aFRR_bids['price'] = pd.to_numeric(aFRR_bids['price'])
    
    grouped = aFRR_bids.groupby('start_time')
    for start_time, group in grouped:
        group_sorted = group.sort_values(by='price', ascending=True)
        group_sorted['cumulative_volume'] = group_sorted['quantity'].cumsum()
        start_times.append(start_time)
        cumulative_volumes.append(group_sorted['cumulative_volume'].values)
        prices.append(group_sorted['price'].values)
    
    aFRR_bids_postprocessed = pd.DataFrame({
        'start_time': pd.to_datetime(start_times),
        'cumulative_volume': cumulative_volumes,
        'price': prices
    }).set_index('start_time')
    
    return aFRR_bids_postprocessed


########################### UTIL ###########################

def convert_to_local_time(utc_time_str, local_tz='Europe/Zurich'):
    """
    Convert a UTC time string to a formatted local time string.
    
    Parameters:
        utc_time_str (str): UTC time string in '%Y-%m-%dT%H:%MZ' format.
        local_tz (str): Local timezone (default 'Europe/Zurich').
    
    Returns:
        str: Local time formatted as '%Y-%m-%d %H:%M:%S'.
    """
    utc_time = datetime.strptime(utc_time_str, '%Y-%m-%dT%H:%MZ')
    utc_time = utc_time.replace(tzinfo=pytz.utc)
    local_time = utc_time.astimezone(pytz.timezone(local_tz))
    return local_time.strftime('%Y-%m-%d %H:%M:%S')