# -*- coding: utf-8 -*-


# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью в МНОГОПОТОЧНОМ стиле
#
# Бумаги с нулевой волатильностью вывести отдельно.
# Результаты вывести на консоль в виде:
#   Максимальная волатильность:
#       ТИКЕР1 - ХХХ.ХХ %
#       ТИКЕР2 - ХХХ.ХХ %
#       ТИКЕР3 - ХХХ.ХХ %
#   Минимальная волатильность:
#       ТИКЕР4 - ХХХ.ХХ %
#       ТИКЕР5 - ХХХ.ХХ %
#       ТИКЕР6 - ХХХ.ХХ %
#   Нулевая волатильность:
#       ТИКЕР7, ТИКЕР8, ТИКЕР9, ТИКЕР10, ТИКЕР11, ТИКЕР12
# Волатильности указывать в порядке убывания. Тикеры с нулевой волатильностью упорядочить по имени.
#
# TODO Внимание! это задание можно выполнять только после зачета lesson_012/01_volatility.py !!!

# TODO тут ваш код в многопоточном стиле
import threading
import os
import csv
import operator


class TickerProcessor(threading.Thread):

    def __init__(self, path_to_file, rate_values, lock,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path_to_file = path_to_file
        self.volatility = {}
        self.rate_values = rate_values
        self.lock = lock

    def get_volatility(self, file):
        reader = csv.DictReader(file)
        for index, line in enumerate(reader):
            price = float(line['PRICE'])
            if index == 0:
                secid = line['SECID']
                total_min = price
                total_max = price
            else:
                if price > total_max:
                    total_max = price
                if price < total_min:
                    total_min = price
        half_sum = (total_max + total_min) / 2
        self.volatility = {secid: round(((total_max - total_min) / half_sum) * 100, 2)}

    def run(self):
        with open(self.path_to_file, 'r') as file:
            self.get_volatility(file)
            with self.lock:
                self.rate_values.update(self.volatility)


def get_files_to_process(path_to_file):
    for (dirpath, dirnames, filenames) in os.walk(path_to_file):
        for filename in filenames:
            if filename.endswith('.csv'):
                yield os.sep.join([dirpath, filename])


def find_top_values(rate_values):
    result = {}
    exclude_zero_values = dict(filter(lambda elem: elem[1] > 0, rate_values.items()))
    result.update({'max_top3_values': dict(sorted(exclude_zero_values.items(), key=operator.itemgetter(1), reverse=True)[:3])})
    result.update({'min_top3_values': dict(sorted(exclude_zero_values.items(), key=operator.itemgetter(1), reverse=False)[:3])})
    zero_values = dict(filter(lambda elem: elem[1] == 0, rate_values.items()))
    result.update({'zero_values_sorted_by_secid': dict(sorted(zero_values.items(), key=operator.itemgetter(0), reverse=False))})
    return result


def print_values(result):
    print('Максимальная волатильность:')
    for item in result['max_top3_values'].items():
        print(f'{item[0]} - {item[1]}%')
    print('')
    print('Минимальная волатильность:')
    for item in result['min_top3_values'].items():
        print(f'{item[0]} - {item[1]}%')
    print('')
    print('Нулевая волатильность:')
    for item in result['zero_values_sorted_by_secid'].items():
        print(f'{item[0]}', end=', ')


def main():
    lock = threading.Lock()
    rate_values = {}
    tps = [TickerProcessor(path_to_file=path_to_file, rate_values=rate_values, lock=lock)
           for path_to_file in get_files_to_process('trades')]
    for tp in tps:
        tp.start()
    for tp in tps:
        tp.join()
    print_values(find_top_values(rate_values))


if __name__ == '__main__':
    main()
# Зачет!

