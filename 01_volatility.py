# -*- coding: utf-8 -*-


# Описание предметной области:
#
# При торгах на бирже совершаются сделки - один купил, второй продал.
# Покупают и продают ценные бумаги (акции, облигации, фьючерсы, етс). Ценные бумаги - это по сути долговые расписки.
# Ценные бумаги выпускаются партиями, от десятка до несколько миллионов штук.
# Каждая такая партия (выпуск) имеет свой торговый код на бирже - тикер - https://goo.gl/MJQ5Lq
# Все бумаги из этой партии (выпуска) одинаковы в цене, поэтому говорят о цене одной бумаги.
# У разных выпусков бумаг - разные цены, которые могут отличаться в сотни и тысячи раз.
# Каждая биржевая сделка характеризуется:
#   тикер ценнной бумаги
#   время сделки
#   цена сделки
#   обьем сделки (сколько ценных бумаг было куплено)
#
# В ходе торгов цены сделок могут со временем расти и понижаться. Величина изменения цен называтея волатильностью.
# Например, если бумага №1 торговалась с ценами 11, 11, 12, 11, 12, 11, 11, 11 - то она мало волатильна.
# А если у бумаги №2 цены сделок были: 20, 15, 23, 56, 100, 50, 3, 10 - то такая бумага имеет большую волатильность.
# Волатильность можно считать разными способами, мы будем считать сильно упрощенным способом -
# отклонение в процентах от полусуммы крайних значений цены за торговую сессию:
#   полусумма = (максимальная цена + минимальная цена) / 2
#   волатильность = ((максимальная цена - минимальная цена) / полусумма) * 100%
# Например для бумаги №1:
#   half_sum = (12 + 11) / 2 = 11.5
#   volatility = ((12 - 11) / half_sum) * 100 = 8.7%
# Для бумаги №2:
#   half_sum = (100 + 3) / 2 = 51.5
#   volatility = ((100 - 3) / half_sum) * 100 = 188.34%
#
# В реальности волатильность рассчитывается так: https://goo.gl/VJNmmY
#
# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью.
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
# Подготовка исходных данных
# 1. Скачать файл https://drive.google.com/file/d/1l5sia-9c-t91iIPiGyBc1s9mQ8RgTNqb/view?usp=sharing
#       (обратите внимание на значок скачивания в правом верхнем углу,
#       см https://drive.google.com/file/d/1M6mW1jI2RdZhdSCEmlbFi5eoAXOR3u6G/view?usp=sharing)
# 2. Раззиповать средствами операционной системы содержимое архива
#       в папку python_base/lesson_012/trades
# 3. В каждом файле в папке trades содержится данные по сделакам по одному тикеру, разделенные запятыми.
#   Первая строка - название колонок:
#       SECID - тикер
#       TRADETIME - время сделки
#       PRICE - цена сделки
#       QUANTITY - количество бумаг в этой сделке
#   Все последующие строки в файле - данные о сделках
#
# Подсказка: нужно последовательно открывать каждый файл, вычитывать данные, высчитывать волатильность и запоминать.
# Вывод на консоль можно сделать только после обработки всех файлов.
#
# Для плавного перехода к мультипоточности, код оформить в обьектном стиле, используя следующий каркас
#
# class <Название класса>:
#
#     def __init__(self, <параметры>):
#         <сохранение параметров>
#
#     def run(self):
#         <обработка данных>

# TODO написать код в однопоточном/однопроцессорном стиле
from threading import Thread
import os
import csv
import operator


class TickerProcessor:

    def __init__(self, path_to_file,  *args, **kwargs):
        self.path_to_file = path_to_file

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
        volatility = round(((total_max - total_min) / half_sum) * 100, 2)
        return {secid: volatility}

    def run(self):
        with open(self.path_to_file, 'r') as file:
            return self.get_volatility(file)


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
    rate_values = {}
    tps = [TickerProcessor(path_to_file=path_to_file) for path_to_file in get_files_to_process('trades')]
    for tp in tps:
        rate_values.update(tp.run())
    result = find_top_values(rate_values)
    print(len(rate_values))
    print_values(result)


if __name__ == '__main__':
    main()
# Зачет!