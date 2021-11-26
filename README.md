# beeline-data-engineer
My realization of beeline data engineer sample task

# Task descriptions

## Book

Написать spark приложение, которое в локальном режиме выполняет следующее:
По имеющимся данным о рейтингах книг посчитать агрегированную статистику по ним.

1. Прочитать csv файл: book.csv
2. Вывести схему для dataframe полученного из п.1
3. Вывести количество записей
4. Вывести информацию по книгам у которых рейтинг выше 4.50
5. Вывести средний рейтинг для всех книг.
6. Вывести агрегированную инфорацию по количеству книг в диапазонах:
0 - 1
1 - 2
2 - 3
3 - 4
4 - 5

## MovieLens

Написать spark приложение, которое в локальном режиме выполняет следующее:
По имеющимся данным о рейтингах фильмов (MovieLens: 100 000 рейтингов) посчитать агрегированную статистику по ним.

## Описание данных
Имеются следующие входные данные:
Архив с рейтингами фильмов.
Файл REDAME содержит описания файлов.
Файл u.data содержит все оценки, а файл u.item — список всех фильмов. (используются только эти два файла)
id_film=32

1. Прочитать данные файлы.
2. создать выходной файл в формате json, где
Поле `“Toy Story ”` нужно заменить на название фильма, соответствующего id_film и указать для заданного фильма количество поставленных оценок в следующем порядке: `"1", "2", "3", "4", "5"`. То есть сколько было единичек, двоек, троек и т.д.
В поле `“hist_all”` нужно указать то же самое только для всех фильмов общее количество поставленных оценок в том же порядке: `"1", "2", "3", "4", "5"`.

Пример решения:

```json
{
   "Toy Story": [ 
      134,
      123,
      782,
      356,
      148
   ],
   "hist_all": [ 
      134,
      123,
      782,
      356,
      148
   ]
}```

# Solution
Run the following command from sbt console to get `Book task`:

```bash
runMain book.Book
```

And you will see result in console output


Run the following command from sbt console to get `Movie task`:

```bash
runMain movie.Movie
```

and you will see genereted json file in `resources/ml100-k` directory

# Specifion:

| App | Value |
| --- | ----------- |
| Scala | 2.12.15 |
| Spark | 3.1.2 |
| OS | Ubuntu |
| javac | 11.0.7 |
| openjdk | 11.0.7 |
| OpenJDK Runtime Environment | 11.0.7+10-post-Ubuntu-2ubuntu219.10 |
| IDE | Visual Studio Code |
| Spark | 3.1.2 |

# Notes
 - All tests have passed
 - Most of the time was spent to setup Spark on Windows, but due to limited of time resource it was decided to move project development to Linux system
 - This project will be updated continiously