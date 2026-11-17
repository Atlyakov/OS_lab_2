import time
import random
import multiprocessing
import sys
from multiprocessing import Value, Lock, Barrier, Process, Array, Queue


def generate_matrix(n, m):
    """Генерация обычной матрицы со случайными целыми числами от 0 до 9"""
    return [[random.randint(0, 9) for _ in range(m)] for _ in range(n)]

def matrix_gen_process(n, m, k, queue):
    """Отдельный процесс-генератор матриц (требование для оценки 4)"""
    A = generate_matrix(n, m)
    B = generate_matrix(m, k)
    queue.put((A, B))


# ==========================================
# 1. ЗАДАНИЕ НА «3»: Стандартное умножение
# ==========================================

def worker_std(n, m, k, A, B, flat_C, start_row, end_row, progress_counter, lock):
    """Рабочий процесс стандартного умножения на быстрых списках Python"""
    for i in range(start_row, end_row):
        row_A = A[i]
        i_k = i * k
        for t in range(m):
            val_a = row_A[t]
            if val_a == 0:
                continue
            row_B = B[t]
            for j in range(k):
                flat_C[i_k + j] += val_a * row_B[j]
        with lock:
            progress_counter.value += 1

def task_grade_3(n, m, k, T):
    print(f"\n--- ОЦЕНКА 3: Умножение матриц ({n}x{m} * {m}x{k}) на {T} процессах ---")
    
    # Генерация матриц в отдельном процессе (IPC через Queue)
    q = Queue()
    p_gen = Process(target=matrix_gen_process, args=(n, m, k, q))
    p_gen.start()
    A, B = q.get()
    p_gen.join()

    # Итоговый результат собирается в разделяемом массиве (п. 1.5 и 1.8)
    flat_C = Array('q', n * k, lock=False)
    counter = Value('i', 0)
    lock = Lock()
    
    rows_per_proc = n // T
    rem = n % T
    procs = []
    start = 0
    
    t_start = time.time()
    for p_id in range(T):
        count = rows_per_proc + (1 if p_id < rem else 0)
        end = start + count
        if start < end:
            pr = Process(target=worker_std, args=(n, m, k, A, B, flat_C, start, end, counter, lock))
            procs.append(pr)
            pr.start()
        start = end

    # Вывод прогресса в реальном времени
    while any(p.is_alive() for p in procs):
        with lock:
            done = counter.value
        pct = int(done * 100 / n)
        sys.stdout.write(f"\rПрогресс: {pct}% ({done}/{n} строк)")
        sys.stdout.flush()
        time.sleep(0.05)

    for pr in procs:
        pr.join()

    elapsed = time.time() - t_start
    chk = sum(flat_C)
    print(f"\rПрогресс: 100% | Время: {elapsed:.4f} с | Контрольная сумма: {chk}")
    return elapsed, chk


# ========================================================
# 2. ЗАДАНИЕ НА «4»: Треугольная матрица + Статик/Динамик
# ========================================================

def matrix_gen_tri_process(n, k, queue):
    """Процесс-генератор для треугольной матрицы"""
    A_tri = [[random.randint(0, 9) for _ in range(i + 1)] for i in range(n)]
    B = [[random.randint(0, 9) for _ in range(k)] for _ in range(n)]
    queue.put((A_tri, B))

def worker_tri_static(n, k, A_tri, B, flat_C, start, end, counter, lock):
    for i in range(start, end):
        row = A_tri[i]
        i_k = i * k
        for t, val in enumerate(row):
            if val == 0:
                continue
            row_B = B[t]
            for j in range(k):
                flat_C[i_k + j] += val * row_B[j]
        with lock:
            counter.value += 1

def worker_tri_dynamic(n, k, A_tri, B, flat_C, shared_idx, counter, lock):
    while True:
        with lock:
            i = shared_idx.value
            if i >= n:
                break
            shared_idx.value += 1
        row = A_tri[i]
        i_k = i * k
        for t, val in enumerate(row):
            if val == 0:
                continue
            row_B = B[t]
            for j in range(k):
                flat_C[i_k + j] += val * row_B[j]
        with lock:
            counter.value += 1

def task_grade_4(n, k, T, mode='static'):
    print(f"\n--- ОЦЕНКА 4: Треугольная матрица ({n}x{n} * {n}x{k}), режим: {mode}, процессов: {T} ---")
    
    q = Queue()
    p_gen = Process(target=matrix_gen_tri_process, args=(n, k, q))
    p_gen.start()
    A_tri, B = q.get()
    p_gen.join()

    flat_C = Array('q', n * k, lock=False)
    counter = Value('i', 0)
    lock = Lock()

    procs = []
    t_start = time.time()

    if mode == 'static':
        rows_per_proc = n // T
        rem = n % T
        start = 0
        for p_id in range(T):
            count = rows_per_proc + (1 if p_id < rem else 0)
            end = start + count
            if start < end:
                pr = Process(target=worker_tri_static, args=(n, k, A_tri, B, flat_C, start, end, counter, lock))
                procs.append(pr)
                pr.start()
            start = end
    else:  # dynamic
        shared_idx = Value('i', 0)
        for p_id in range(T):
            pr = Process(target=worker_tri_dynamic, args=(n, k, A_tri, B, flat_C, shared_idx, counter, lock))
            procs.append(pr)
            pr.start()

    while any(p.is_alive() for p in procs):
        with lock:
            done = counter.value
        pct = int(done * 100 / n)
        sys.stdout.write(f"\rПрогресс ({mode}): {pct}% ({done}/{n} строк)")
        sys.stdout.flush()
        time.sleep(0.05)

    for pr in procs:
        pr.join()

    elapsed = time.time() - t_start
    chk = sum(flat_C)
    print(f"\rПрогресс ({mode}): 100% | Время: {elapsed:.4f} с | Контрольная сумма: {chk}")
    return elapsed


# ========================================================
# 3. ЗАДАНИЕ НА «5»: Возведение в степень с барьером
# ========================================================

def matrix_gen_square_process(n, queue):
    """Отдельный процесс-генератор квадратной матрицы (IPC через Queue)"""
    A = generate_matrix(n, n)
    queue.put(A)

def power_worker_target(pid, n, p_power, A, buf1, buf2, start, end, barrier, counter, lock, step_flag):
    for step in range(p_power - 1):
        is_buf1 = (step_flag.value == 0)
        src = buf1 if is_buf1 else buf2
        dst = buf2 if is_buf1 else buf1

        # Обнуление своей части строк в целевом буфере
        for i in range(start, end):
            i_n = i * n
            for j in range(n):
                dst[i_n + j] = 0

        # Умножение текущей матрицы на исходную A с защитой по модулю (п. 4.1)
        for i in range(start, end):
            i_n = i * n
            for t in range(n):
                val = src[i_n + t]
                if val == 0:
                    continue
                row_A = A[t]
                for j in range(n):
                    dst[i_n + j] = (dst[i_n + j] + val * row_A[j]) % 1000000007
            with lock:
                counter.value += 1

        # Синхронизация через барьер
        barrier.wait()
        if pid == 0:
            step_flag.value = 1 - step_flag.value
        barrier.wait()

def task_grade_5(n, p_power, T):
    print(f"\n--- ОЦЕНКА 5: Возведение матрицы {n}x{n} в степень {p_power} на {T} процессах с барьером ---")
    
    # Генерация матрицы в отдельном процессе (IPC через Queue)
    q = Queue()
    p_gen = Process(target=matrix_gen_square_process, args=(n, q))
    p_gen.start()
    A = q.get()
    p_gen.join()
    
    buf1 = Array('q', n * n, lock=False)
    buf2 = Array('q', n * n, lock=False)

    for i in range(n):
        for j in range(n):
            buf1[i * n + j] = A[i][j]  # R_1 = A

    counter = Value('i', 0)
    lock = Lock()
    step_flag = Value('i', 0)
    barrier = Barrier(T)

    rows_per_proc = n // T
    rem = n % T
    procs = []
    start = 0

    t_start = time.time()
    for p_id in range(T):
        count = rows_per_proc + (1 if p_id < rem else 0)
        end = start + count
        if start < end:
            pr = Process(
                target=power_worker_target,
                args=(p_id, n, p_power, A, buf1, buf2, start, end, barrier, counter, lock, step_flag)
            )
            procs.append(pr)
            pr.start()
        start = end

    total_ops_rows = (p_power - 1) * n
    while any(p.is_alive() for p in procs):
        with lock:
            done = counter.value
        pct = int(min(100, done * 100 / total_ops_rows))
        sys.stdout.write(f"\rПрогресс степени: {pct}% ({done}/{total_ops_rows} шагов)")
        sys.stdout.flush()
        time.sleep(0.05)

    for pr in procs:
        pr.join()

    elapsed = time.time() - t_start
    
    # Определяем актуальный результирующий буфер для подсчёта контрольной суммы
    final_buf = buf1 if step_flag.value == 0 else buf2
    chk = sum(final_buf)
    print(f"\rПрогресс степени: 100% | Время: {elapsed:.4f} с | Контрольная сумма: {chk}")
    return elapsed, chk


if __name__ == '__main__':
    multiprocessing.freeze_support()
    print("=== Выполнение лабораторной работы по ОС (Оценка 5) ===")
    
    # Размеры матриц и кол. процессов при запуске
    MATRIX_SIZE = 300
    PROCESSES = 4
    
    # 1. Задание на «3»
    task_grade_3(MATRIX_SIZE, MATRIX_SIZE, MATRIX_SIZE, PROCESSES)
    
    # 2. Задание на «4» (статическое и динамическое распределение)
    task_grade_4(MATRIX_SIZE, MATRIX_SIZE, PROCESSES, mode='static')
    task_grade_4(MATRIX_SIZE, MATRIX_SIZE, PROCESSES, mode='dynamic')
    
    # 3. Задание на «5» (возведение в степень с барьером и модулем)
    task_grade_5(n=300, p_power=4, T=PROCESSES)