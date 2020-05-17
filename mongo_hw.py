import csv
import re
from pymongo import MongoClient
import datetime as dt

def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf-8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        list_of_artists = list(reader)
        list_for_db = []
        for i, artist in enumerate(list_of_artists, 1):
            info = {'_id': i,
                           'Исполнитель': artist['Исполнитель'],
                           'Цена': int(artist['Цена']),
                           'Место': artist['Место'],
                           'Дата': dt.datetime.strptime('2020 ' + artist['Дата'], '%Y %d.%m')}

            list_for_db.append(info)

        db.insert_many(list_for_db)


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    tikets = list(db.find().sort('Цена', 1))
    for t in tikets:
        print(t)


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    regex = re.compile(name, re.IGNORECASE)
    result = list(db.find({'Исполнитель': regex}).sort('Цена', 1))
    for line in result:
        print(line)


def find_by_date(db):
    """
    Сортировка по дате мероприятия, с 1 по 30 июля,  $gte >=, $lte <=
    """
    start = dt.datetime(2020, 7, 1)
    end = dt.datetime(2020, 7, 30)
    result = list(db.find({'Дата': {'$gte': start, '$lte': end}}).sort('Дата', 1))
    for line in result:
        print(line)


if __name__ == '__main__':
    client = MongoClient()
    netology_db = client['netology']
    artists_collection = netology_db['artists']
    artists_collection.drop()
    read_data('artists.csv', artists_collection)

    print("Sort by cheapest")
    find_cheapest(artists_collection)

    print("Search by name")
    find_by_name('Seconds to', artists_collection)

    print("Search by date")
    find_by_date(artists_collection)
