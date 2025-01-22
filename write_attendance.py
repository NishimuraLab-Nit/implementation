def judge_attendance_for_course(entry_dt, exit_dt, start_dt, finish_dt):
    """
    出席判定ロジック(早退を含む):
      1) 欠席(×):
         entry_dt >= finish_dt
      2) 早退(△早):
         entry_dt <= (start_dt + 5分) かつ exit_dt < (finish_dt - 5分)
         → "△早xx分" (xx = finish_dt - exit_dt in 分)
      3) 遅刻(△遅):
         entry_dt > (start_dt + 5分) かつ exit_dt <= (finish_dt + 5分)
         → "△遅xx分" (xx = entry_dt - start_dt in 分)
      4) 正常(○):
         ① entry_dt <= start_dt+5分 かつ exit_dt <= finish_dt+5分
         ② exit_dt > finish_dt+5分 → exit=finish, 次コマ entry=finish+10分, exit=original_exit
         ③ exit_dt=None → exit=finish, 次コマ entry=finish+10分
      5) 上記いずれでもない → "？"

    戻り値:
      (status_str, updated_entry_dt, updated_exit_dt, next_course_data)
        - status_str: 出席ステータス ("×", "○", "△早xx分", "△遅xx分", など)
        - updated_entry_dt, updated_exit_dt: 今コースの entry, exit を補正した場合に返す
        - next_course_data: (next_entry_dt, next_exit_dt) or None
          → (②,③ の場合に次コマの entry/exit を作成して返す)
    """
    import datetime
    td_5min  = datetime.timedelta(minutes=5)
    td_10min = datetime.timedelta(minutes=10)

    # (1) 欠席(×)
    if entry_dt >= finish_dt:
        return "×", entry_dt, exit_dt, None

    # (2) 早退(△早)
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt < (finish_dt - td_5min)):
        # xx分 = finish_dt - exit_dt
        delta_min = int((finish_dt - exit_dt).total_seconds() // 60)
        return f"△早{delta_min}分", entry_dt, exit_dt, None

    # (3) 遅刻(△遅)
    if (entry_dt > (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt <= (finish_dt + td_5min)):
        # xx分 = entry_dt - start_dt
        delta_min = int((entry_dt - start_dt).total_seconds() // 60)
        return f"△遅{delta_min}分", entry_dt, exit_dt, None

    # (4) 正常(○) ①
    if (entry_dt <= (start_dt + td_5min)) and (exit_dt is not None) and (exit_dt <= (finish_dt + td_5min)):
        return "○", entry_dt, exit_dt, None

    # (4) 正常(○) ②: exit > finish+5分
    if (exit_dt is not None) and (exit_dt > (finish_dt + td_5min)):
        status_str = "○"
        original_exit = exit_dt
        updated_exit_dt = finish_dt
        # 次コマ entry = finish_dt + 10分
        next_entry_dt = finish_dt + td_10min
        # 次コマ exit = original_exit
        next_exit_dt  = original_exit
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # (4) 正常(○) ③: exit=None
    if exit_dt is None:
        status_str = "○"
        updated_exit_dt = finish_dt
        # 次コマ entry = finish_dt + 10分
        next_entry_dt = finish_dt + td_10min
        # 次コマ exit=None
        next_exit_dt = None
        return status_str, entry_dt, updated_exit_dt, (next_entry_dt, next_exit_dt)

    # (5) どれにも当てはまらない場合 → "？"
    return "？", entry_dt, exit_dt, None
