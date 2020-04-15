                                           **ИНСТРУКЦИЯ**
                                
1. pip install -r requirements.txt `Установка зависимостей`
2. python all_section.py `Запуск скрипта формирования файла all_section.json`
3. python all_section.py update `Запуск скрипта обновления файла all_section.json`
4. python generate_query.py `Запуск скрипта формирования файла main.json с url второго уровня из all_section.json и кластеризация в excel`
5. python generate_query.py manual_keys `Запуск скрипта формирования excel файла main.json с ключами из keys.txt и кластеризация в excel`
6  python generate_query.py manual_links `Запуск скрипта формирования excel файла main.json с ссылками из links.txt и кластеризация в excel`