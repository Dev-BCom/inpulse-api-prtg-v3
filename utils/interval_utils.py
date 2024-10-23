from datetime import datetime, timedelta

def group_data_into_intervals(device_info, import_start_date):
    """
    Groups 'day's into intervals of consecutive days, max 1 month
    """
    # device_info is a list of dicts with 'day' and 'modified'
    # We need to group 'day's into intervals of consecutive days, max 1 month

    # First, parse 'day's into datetime objects using the correct format
    days = []
    for item in device_info:
        day_str = item.get('day')
        if not day_str:
            continue
        try:
            # Adjusted date format to match "YYYYMMDD"
            day_date = datetime.strptime(day_str, "%Y%m%d")
            # Exclude dates before import_start_date
            if day_date >= import_start_date:
                days.append(day_date)
        except ValueError:
            # Handle cases where day is not in expected format
            continue

    # Sort the days
    days.sort()

    # Now group into intervals
    intervals = []
    if not days:
        return intervals

    start_day = days[0]
    end_day = start_day
    max_interval = timedelta(days=30)

    for current_day in days[1:]:
        if (current_day - end_day).days == 1 and (current_day - start_day) <= max_interval:
            # Consecutive day and within max interval
            end_day = current_day
        else:
            # Start a new interval
            intervals.append((start_day, end_day))
            start_day = current_day
            end_day = current_day

    # Append the last interval
    intervals.append((start_day, end_day))

    return intervals
