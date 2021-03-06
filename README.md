# Задание

Есть веб-api, непрерывно принимающее события (ограничимся 10000 событий) для группы аккаунтов (1000 аккаунтов) и складывающее их в очередь.
Каждое событие связано с определенным аккаунтом и важно, чтобы события аккаунта обрабатывались в том же порядке, в котором поступили в очередь.
Обработка события занимает 1 секунду (эмулировать с помощью sleep).

Веб-api и очередь эмулировать консольным скриптом, непрерывно генерирующим события для обработки в фоне. События генерировать пачками, содержащими последовательности по 1-10 для каждого аккаунта.
Сделать обработку очереди событий максимально быстрой на данной конкретной машине.

Код писать на PHP. Можно использовать фреймворки и инструменты такие как RabbitMQ, Redis, MySQL и т. д. Запуск кода сделать в 1-3 команды, поэтому при необходимости использовать docker и docker-compose.

На выходе нужен лог с id аккаунтов и id событий для проверки очередности выполнения событий.

# Событие
{номер события: номер аккаунта}

# events.php
Лог: events.log

Сами события: events/*.json

Генератор "событий" в файлы. Для эмуляции последовательного поступления события файлы создаются после секундной задержки.

# daemon.php
Демон, обрабатывающий получаемые события. Берет файлы из events в порядке очереди (первый пришел - первый обработался), опираясь на время создания файла.

Обработанные (добавленные в очередь родительского процесса) чанки-пачки событий переносятся в processed_events с теми же названиями
Лог: daemon.log

Демон "состоит" из процесса родителя, который управляет:

- пополнением внутренней очереди, 
- блокировкой уже обрабатываемых аккаунтов, 
- созданием дочерних процессов, 
- созданием пачек заданий для них (json-файлы в threads с именем PID процесса-получателя), 
- проверкой завершения их работы 
- освобождением блокируемых процессов и аккаунтов.

Дочерний процесс просто последовательно обрабатывает ( sleep(1) ) полученные сообщения и пишет в лог об этом.

- В чанке для процесса могут быть события для одного аккаунта, так как это не противоречит последовательному выполнению для аккаунта.

# Запуск

php events.php &

php daemon.php

# Результат

- Генератора событий: events.log (или файлы в events | processed_events )
- Демона-обработчика: daemon.log

# Быстродействие

10 тысяч событий  для 1000 аккаунтов (генерируется функцией rand, так что условно-случайные) чанками по 50-100 закидываются в файлы каждую секунду, пока не сгенерируем n файлов со всеми событиями.

Генерация в лучшем случае займет 100 секунд. Но это неважно, т.к. даже первые несколько чанков займут дочерние процессы на большее время.

Разбор очереди зависит от состава очереди, от скорости ее генерации, числа дочерних процессов и числа событий в чанке, который выделяется на один процесс.

- Сильно много процессов плодить смысла не имеет, т.к. 1 процесс освободится раньше, чем успеет создаться чанк для процесса N.
- Чанк должен быть не сильно маленьким, чтобы снизить временные расходы на запись и чтение, но и не сильно большим, чтобы выполнение было "более параллельным"

Для числа дочерних процессов 10-50 и чанка 5-10 обработка очереди в нескольких тестах заняла 660-780 секунд.

- ~14 раз быстрее последовательной обработки
- ~3.5 медленнее идеальной ситуации, когда для всех 50 дочерних процессов найдется работа каждую секунду их существования

# Полезные команды

kill -9  $(ps aux | grep daemon.php | awk '{print $2}')

rm -f processed_events/* threads/* events/* daemon.log events.log

cat events.log

tail -f daemon.log

# Ошибки

Лог: error.log
