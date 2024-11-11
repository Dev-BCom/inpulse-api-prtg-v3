# utils/interval_utils.py

from datetime import datetime, timedelta

def group_data_into_intervals(device_info, import_start_date, max_interval_days=7):
    days = []
    for item in device_info:
        day_str = item.get('day')
        if not day_str:
            continue
        try:
            day_date = datetime.strptime(day_str, "%Y%m%d").date()  # Only get the date part
            if day_date >= import_start_date.date():  # Compare only the date part
                days.append(day_date)
        except ValueError:
            continue

    days.sort()

    intervals = []
    if not days:
        return intervals

    start_day = days[0]
    end_day = start_day
    max_interval = timedelta(days=max_interval_days)

    for current_day in days[1:]:
        if (current_day - end_day).days == 1 and (current_day - start_day) <= max_interval:
            end_day = current_day
        else:
            intervals.append((start_day, end_day))
            start_day = current_day
            end_day = current_day

    intervals.append((start_day, end_day))

    return intervals

