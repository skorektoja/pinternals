Быстрые заметки по архитектуре diffo.

1. Все методы чётко и явно разделаются на извлекающие информацию онлайн (не зависят от БД) и из БД (не зависят от онлайна). Смешение подходов внутри одного метода не допускается.

2. Существуют высокоуровневые методы, запрашивающие информацию из БД [и онлайна и затем смешивающие результаты, если был запрос онлайн].

3. Все методы используют единую очередь HTTP-тредов.

4. Использование многотредности поощряется.