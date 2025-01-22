# (2) exit > finish+5分 のケース
if exit_dt > (finish_dt + td_5min):
    status_str = "○"
    original_exit_dt = exit_dt
    updated_exit_dt = finish_dt

    # 修正前: next_course_entry_dt = finish_dt + td_10min
    # 修正後:
    temp_dt = finish_dt + td_10min
    forced_next_dt = datetime.datetime(
        finish_dt.year,
        finish_dt.month,
        finish_dt.day,
        temp_dt.hour,
        temp_dt.minute,
        temp_dt.second
    )
    next_course_entry_dt = forced_next_dt
