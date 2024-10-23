import logging
import re

def parse_prtg_response(text):
    """
    Parses the PRTG API response text and handles duplicate keys by organizing them into arrays.
    """
    # Remove newline characters
    text = text.replace('\n', '')

    # Use regex to find the 'histdata' array
    match = re.search(r'"histdata":\s*(\[\{.*?\}\])', text)
    if not match:
        logging.error("No 'histdata' found in PRTG API response.")
        return {}

    histdata_text = match.group(1)

    # Split the histdata_text into individual items
    items_text = re.findall(r'\{(.*?)\}(?:,|$)', histdata_text)
    histdata = []

    for item_text in items_text:
        # Initialize an empty dictionary for each item
        item = {}
        # Prepare lists to collect duplicate keys
        fields = {}
        # Split the item text into key-value pairs
        pairs = re.findall(r'("(?:\\.|[^"\\])*?"\s*:\s*(?:"(?:\\.|[^"\\])*?"|-?\d+(?:\.\d+)?))(?:,|$)', item_text)

        for pair in pairs:
            # Split key and value
            if ':' not in pair:
                continue
            key, val = pair.split(':', 1)
            key = key.strip().strip('"')
            val = val.strip()
            # Remove wrapping quotes from val if present
            if val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
                val = val.replace('\\"', '"').replace('\\\\', '\\')
            else:
                val = val.strip('"')
            # Collect duplicate keys into lists
            if key in fields:
                if isinstance(fields[key], list):
                    fields[key].append(val)
                else:
                    fields[key] = [fields[key], val]
            else:
                fields[key] = val

        # Now process the collected fields
        # For keys that have lists, keep them as lists
        # For keys that have single values, keep them as is
        # Convert numeric strings to appropriate types
        for key, val in fields.items():
            if key in ['value', 'value_raw']:
                if not isinstance(val, list):
                    val = [val]
                item[key] = val
            else:
                # Attempt to convert to float
                try:
                    item[key] = float(val)
                except ValueError:
                    item[key] = val
        # Convert 'value_raw' elements to float if possible
        if 'value_raw' in item:
            new_value_raw = []
            for v in item['value_raw']:
                try:
                    new_value_raw.append(float(v))
                except ValueError:
                    new_value_raw.append(v)
            item['value_raw'] = new_value_raw

        # Remove degree symbol from 'value' entries
        item['value'] = [v.replace('°', '') for v in item.get('value', [])]

        histdata.append(item)

    # Return the parsed data
    return {'histdata': histdata}


