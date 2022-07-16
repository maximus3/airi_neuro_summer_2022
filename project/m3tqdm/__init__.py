import time


def get_need_time(total, current, avg_speed):
    return time.strftime('%X', time.gmtime((total - current) / avg_speed))


def tqdm(iterable, total=None, end='', name='', logger=None, tmp_filename=None):
    if name:
        name = '>>' + name + '\t'
    if total is None:
        try:
            total = len(iterable)
        except TypeError:
            total = None
    current = 0
    iter_obj = iter(iterable)
    all_time = '00:00:00'
    avg_speed = 1
    start_time = time.time()
    max_len = 0
    while True:
        need_time = get_need_time(total, current, avg_speed)
        need_time_for_all = get_need_time(total, 0, avg_speed)
        avg_data = f'{1 / avg_speed:.2f} s/it' if avg_speed < 1 else f'{avg_speed:.2f} it/s'
        text = f'{name}[{current}/{total}]\t{need_time}/{need_time_for_all}\t{avg_data}\t{all_time}'
        max_len = max(max_len, len(text) + 24)
        if logger:
            logger.info(text)
        else:
            print('\r', ' ' * max_len, '\r', sep='', end='')
            print(f'\r{text}\r', end=end)
        if tmp_filename:
            with open(tmp_filename, 'w') as f:
                f.write(text)
        all_time = time.strftime('%X', time.gmtime(time.time() - start_time))
        try:
            current += 1
            yield next(iter_obj)
            avg_speed = current / max(0.001, time.time() - start_time)
        except StopIteration:
            break

    if tmp_filename:
        tmp_filename.unlink()
