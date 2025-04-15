import multiprocessing
import random
import logging
import os
import threading
import psutil 

logging.basicConfig(filename='sort.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(process)d - %(threadName)s - %(message)s')

def simple_sort(arr):
    n = len(arr)
    for i in range(n):
        for j in range(0, n - i - 1):
            if arr[j] > arr[j + 1]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
    return arr

def save_part(output_filename, data):
    with open(output_filename, 'w') as f:
        f.write(' '.join(map(str, data)) + '\n')
    logging.info(f'Saved sorted array to {output_filename}')


def merge_sorted_arrays(sorted_arrays):
    merged_array = []
    for arr in sorted_arrays:
        merged_array.extend(arr)
    return simple_sort(merged_array)

def worker(input_queue, data, process_index):
    sorted_parts = []
    while True:
        start, end = input_queue.get()
        if start == end == -1:
            break

        logging.info(f'Sorting from index {start} to {end}')
        result = simple_sort(data[start:end])
        sorted_parts.append(result)

        output_filename = f'sorted_part_{process_index}.txt'
        saver_thread = threading.Thread(target=save_part, args=(output_filename, result))
        saver_thread.start()

    return sorted_parts


def main():
    num_cpus = multiprocessing.cpu_count()
    cpu_percent = psutil.cpu_percent()
    available_cpus = int(num_cpus * (1 - cpu_percent / 100) * 0.75) 

    user_input_elements = int(input(f'Введите количество элементов (больше 0): '))
    if user_input_elements <= 0:
        print("Количество элементов должно быть положительным.")
        return

    user_input_processes = int(input(f'Введите количество процессов (до {available_cpus}): '))
    num_processes = min(user_input_processes, available_cpus)

    if num_processes <= 0:
        print("Недостаточно доступных ресурсов CPU для запуска процессов.")
        return

    data = [random.randint(0, 1000) for _ in range(user_input_elements)]
    logging.info(f'Generated array: {data}')

    input_queue = multiprocessing.Queue()
    processes = []
    sorted_parts = []
    
    for i in range(num_processes):
        p = multiprocessing.Process(target=worker, args=(input_queue, data, i))
        processes.append(p)
        p.start()

    chunk_size = user_input_elements // num_processes
    for i in range(num_processes):
        start = i * chunk_size
        end = start + chunk_size if i < num_processes - 1 else user_input_elements
        input_queue.put((start, end))

    for i in range(num_processes):
        input_queue.put((-1, -1))

    for p in processes:
        p.join()

    sorted_parts = [open(f'sorted_part_{i}.txt').read().strip().split() for i in range(num_processes)]
    sorted_parts = [list(map(int, arr)) for arr in sorted_parts]
    
    final_sorted_array = merge_sorted_arrays(sorted_parts)

    final_output_filename = 'final_sorted_array.txt'
    save_part(final_output_filename, final_sorted_array)

    print("Сортировка завершена.")


if __name__ == '__main__':
    main()
